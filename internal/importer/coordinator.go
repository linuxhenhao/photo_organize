package importer

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/linuxhenhao/photo_organize/internal/dedupe"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/linuxhenhao/photo_organize/internal/precompute"
	"github.com/linuxhenhao/photo_organize/internal/target"
)

type importPlanAction int

const (
	importPlanSkip importPlanAction = iota
	importPlanWait
	importPlanCopyMaster
	importPlanCopyThumbnail
)

type importPlan struct {
	action      importPlanAction
	waitCh      <-chan struct{}
	reservation *importReservation
}

type importReservation struct {
	seq                uint64
	task               ImportTask
	finalPath          string
	hasPHash           bool
	phash              uint64
	sourceMeta         string
	action             importPlanAction
	committedMatchPath string
	done               chan struct{}
}

type inflightCompareCandidate struct {
	done            <-chan struct{}
	comparePath     string
	compareMetaJSON string
	compareSize     int64
	compareMMH3     string
	compareDHash    uint64
	compareHasDHash bool
	distance        int
}

type confirmedImportMatch struct {
	match hasher.MatchResult
}

type importCoordinator struct {
	cacheManager    *target.CacheManager
	featureResolver *precompute.Resolver

	mutex          sync.Mutex
	nextSeq        uint64
	inflight       map[uint64]*importReservation
	inflightByMMH3 map[string]*importReservation
	inflightByPath map[string]*importReservation
}

func newImportCoordinator(cacheManager *target.CacheManager, featureResolver *precompute.Resolver) *importCoordinator {
	return &importCoordinator{
		cacheManager:    cacheManager,
		featureResolver: featureResolver,
		inflight:        make(map[uint64]*importReservation),
		inflightByMMH3:  make(map[string]*importReservation),
		inflightByPath:  make(map[string]*importReservation),
	}
}

func parseTaskPHash(task ImportTask) (uint64, bool) {
	if task.PHash == "" || task.PHash == "UNSUPPORTED" || task.PHash == "NOT_IMAGE" {
		return 0, false
	}
	phash, err := hasher.StringToPHash(task.PHash)
	if err != nil {
		return 0, false
	}
	return phash, true
}

func (c *importCoordinator) planTask(ctx context.Context, task ImportTask, sourceMeta string) importPlan {
	phash, hasPHash := parseTaskPHash(task)

	var inflightCandidates []inflightCompareCandidate
	for {
		c.mutex.Lock()
		if _, found := c.findCommittedExactMatchLocked(task.MMH3Hash); found {
			c.mutex.Unlock()
			return importPlan{action: importPlanSkip}
		}

		if task.MMH3Hash != "" {
			if _, found := c.inflightByMMH3[task.MMH3Hash]; found {
				c.mutex.Unlock()
				return importPlan{action: importPlanSkip}
			}
		}

		if hasPHash {
			inflightCandidates = c.snapshotInflightCandidatesLocked(phash)
		} else {
			inflightCandidates = nil
		}
		c.mutex.Unlock()

		if hasPHash {
			if waitCh := c.findInflightVisualMatch(ctx, task, sourceMeta, phash, inflightCandidates); waitCh != nil {
				return importPlan{action: importPlanWait, waitCh: waitCh}
			}
		}

		var committedMatch *confirmedImportMatch
		if hasPHash {
			committedMatch = c.findCommittedVisualMatch(ctx, task, sourceMeta, phash)
		}

		c.mutex.Lock()
		// Re-check after expensive work since other workers may have committed in the meantime.
		if _, found := c.findCommittedExactMatchLocked(task.MMH3Hash); found {
			c.mutex.Unlock()
			return importPlan{action: importPlanSkip}
		}
		if task.MMH3Hash != "" {
			if _, found := c.inflightByMMH3[task.MMH3Hash]; found {
				c.mutex.Unlock()
				return importPlan{action: importPlanSkip}
			}
		}

		masterTargetPath := c.resolveAvailableTargetPathLocked(task.TargetDir, task.FileName)
		if committedMatch != nil {
			thumbDir := filepath.Join(
				targetDirRoot(task.TargetDir),
				"thumbnails",
				filepath.Base(filepath.Dir(filepath.Dir(task.TargetDir))),
				filepath.Base(filepath.Dir(task.TargetDir)),
				filepath.Base(task.TargetDir),
			)
			thumbTargetPath := c.resolveAvailableTargetPathLocked(thumbDir, task.FileName)
			reservation := c.reserveLocked(task, sourceMeta, thumbTargetPath, phash, true, importPlanCopyThumbnail, committedMatch.match.Path)
			c.mutex.Unlock()
			return importPlan{action: importPlanCopyThumbnail, reservation: reservation}
		}

		reservation := c.reserveLocked(task, sourceMeta, masterTargetPath, phash, hasPHash, importPlanCopyMaster, "")
		c.mutex.Unlock()
		return importPlan{action: importPlanCopyMaster, reservation: reservation}
	}
}

func (c *importCoordinator) reserveLocked(task ImportTask, sourceMeta string, finalPath string, phash uint64, hasPHash bool, action importPlanAction, committedMatchPath string) *importReservation {
	c.nextSeq++
	reservation := &importReservation{
		seq:                c.nextSeq,
		task:               task,
		finalPath:          finalPath,
		hasPHash:           hasPHash,
		phash:              phash,
		sourceMeta:         sourceMeta,
		action:             action,
		committedMatchPath: committedMatchPath,
		done:               make(chan struct{}),
	}
	c.inflight[reservation.seq] = reservation
	c.inflightByPath[reservation.finalPath] = reservation
	if reservation.task.MMH3Hash != "" {
		c.inflightByMMH3[reservation.task.MMH3Hash] = reservation
	}
	return reservation
}

func (c *importCoordinator) cancelReservation(reservation *importReservation) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.removeReservationLocked(reservation)
	close(reservation.done)
}

func (c *importCoordinator) commitReservation(reservation *importReservation) string {
	finalPath := reservation.finalPath

	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch reservation.action {
	case importPlanCopyMaster:
		c.cacheManager.AddEntryWithPresence(
			finalPath,
			reservation.task.MMH3Hash,
			reservation.phash,
			reservation.hasPHash,
			reservation.task.Size,
			reservation.sourceMeta,
		)
	case importPlanCopyThumbnail:
		if c.isCommittedPathValidLocked(reservation.committedMatchPath) {
			c.cacheManager.AppendThumbnailToMaster(reservation.committedMatchPath, finalPath, reservation.sourceMeta)
		} else {
			recoveryPath := c.resolveAvailableTargetPathLocked(reservation.task.TargetDir, reservation.task.FileName)
			if recoveryPath != finalPath {
				if err := os.MkdirAll(filepath.Dir(recoveryPath), 0755); err == nil {
					if err := os.Rename(finalPath, recoveryPath); err == nil {
						finalPath = recoveryPath
					} else {
						log.Printf("Failed to recover thumbnail [%s] to master path [%s]: %v", finalPath, recoveryPath, err)
					}
				}
			}
			c.cacheManager.AddEntryWithPresence(
				finalPath,
				reservation.task.MMH3Hash,
				reservation.phash,
				reservation.hasPHash,
				reservation.task.Size,
				reservation.sourceMeta,
			)
		}
	}

	c.removeReservationLocked(reservation)
	close(reservation.done)
	return finalPath
}

func (c *importCoordinator) removeReservationLocked(reservation *importReservation) {
	delete(c.inflight, reservation.seq)
	if current, ok := c.inflightByPath[reservation.finalPath]; ok && current == reservation {
		delete(c.inflightByPath, reservation.finalPath)
	}
	if reservation.task.MMH3Hash != "" {
		if current, ok := c.inflightByMMH3[reservation.task.MMH3Hash]; ok && current == reservation {
			delete(c.inflightByMMH3, reservation.task.MMH3Hash)
		}
	}
}

func (c *importCoordinator) resolveAvailableTargetPathLocked(targetDir, fileName string) string {
	candidate := filepath.Join(targetDir, fileName)
	if !c.isPathOccupiedLocked(candidate) {
		return candidate
	}

	ext := filepath.Ext(fileName)
	nameWithoutExt := fileName[:len(fileName)-len(ext)]
	for suffix := 1; ; suffix++ {
		candidate = filepath.Join(targetDir, fileNameWithSuffix(nameWithoutExt, suffix, ext))
		if !c.isPathOccupiedLocked(candidate) {
			return candidate
		}
	}
}

func fileNameWithSuffix(name string, suffix int, ext string) string {
	return fmt.Sprintf("%s-%d%s", name, suffix, ext)
}

func (c *importCoordinator) isPathOccupiedLocked(path string) bool {
	if _, ok := c.inflightByPath[path]; ok {
		return true
	}

	if c.cacheManager.IsCached(path) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		log.Printf("Removing stale cache entry for missing committed path [%s]", path)
		c.cacheManager.DeleteEntry(path)
		return false
	}

	_, err := os.Stat(path)
	return err == nil
}

func (c *importCoordinator) isCommittedPathValidLocked(path string) bool {
	if path == "" {
		return false
	}
	if !c.cacheManager.IsCached(path) {
		return false
	}
	if _, err := os.Stat(path); err == nil {
		return true
	}
	log.Printf("Removing stale cache entry for missing committed path [%s]", path)
	c.cacheManager.DeleteEntry(path)
	return false
}

func (c *importCoordinator) findCommittedExactMatchLocked(mmh3 string) (string, bool) {
	for {
		path, found := c.cacheManager.FindExactMatch(mmh3)
		if !found {
			return "", false
		}
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
		log.Printf("Removing stale exact-match cache entry for missing path [%s]", path)
		c.cacheManager.DeleteEntry(path)
	}
}

func resolveImportFeatures(ctx context.Context, resolver *precompute.Resolver, mmh3 string, dhash uint64, hasDHash bool, absPath string) dedupe.ResolvedVisualFeatures {
	features := dedupe.ResolvedVisualFeatures{
		DHash:    dhash,
		HasDHash: hasDHash,
	}
	if resolver == nil || mmh3 == "" {
		return features
	}

	resolved, _, err := resolver.ResolveOrCompute(ctx, mmh3, absPath)
	if err != nil || resolved == nil {
		return features
	}

	features.ColorSignature = resolved.ColorSignature
	features.HasColorSignature = resolved.HasColorSignature
	features.ORB = resolved.ORB
	return features
}

func (c *importCoordinator) findCommittedVisualMatch(ctx context.Context, task ImportTask, sourceMeta string, phash uint64) *confirmedImportMatch {
	matches := c.cacheManager.SearchPHash(phash, dedupe.CandidateSearchDistance)
	var best *confirmedImportMatch
	var bestMeta metadata.MediaMeta
	ambiguous := false

	childFeatures := resolveImportFeatures(ctx, c.featureResolver, task.MMH3Hash, phash, true, task.SourcePath)
	for _, candidate := range matches {
		if _, err := os.Stat(candidate.Path); err != nil {
			log.Printf("Removing stale perceptual match entry for missing path [%s]", candidate.Path)
			c.cacheManager.DeleteEntry(candidate.Path)
			continue
		}

		existingMeta := metadata.ExtractImageMetaJson(candidate.Path)

		info, ok := c.cacheManager.GetCachedInfo(candidate.Path)
		candidateMMH3 := ""
		if ok {
			candidateMMH3 = info.MMH3
		}
		if candidateMMH3 == "" {
			if computed, computeErr := hasher.CalculateHash(candidate.Path); computeErr == nil {
				candidateMMH3 = computed
				c.cacheManager.AddEntryWithPresence(candidate.Path, computed, candidate.Hash, true, candidate.Size, existingMeta)
			}
		}
		parentFeatures := resolveImportFeatures(ctx, c.featureResolver, candidateMMH3, candidate.Hash, true, candidate.Path)

		decision, err := dedupe.ClassifyDerivativeWithResolvedFeatures(task.SourcePath, sourceMeta, task.Size, childFeatures, candidate.Path, existingMeta, candidate.Size, parentFeatures)
		if err != nil {
			log.Printf("Failed to confirm visual duplicate [%s] against [%s]: %v", task.SourcePath, candidate.Path, err)
			continue
		}
		if decision.Confirmed {
			candidateMeta := metadata.ParseMediaMetaJSON(existingMeta)
			if best == nil {
				best = &confirmedImportMatch{match: candidate}
				bestMeta = candidateMeta
				ambiguous = false
				continue
			}

			cmp := dedupe.CompareMasterPreference(candidate.Path, candidateMeta, candidate.Size, best.match.Path, bestMeta, best.match.Size)
			if cmp > 0 || (cmp == 0 && candidate.Distance < best.match.Distance) {
				best = &confirmedImportMatch{match: candidate}
				bestMeta = candidateMeta
				ambiguous = false
				continue
			}
			if cmp == 0 && candidate.Distance == best.match.Distance {
				ambiguous = true
			}
		}
	}

	if ambiguous {
		return nil
	}
	return best
}

func (c *importCoordinator) snapshotInflightCandidatesLocked(phash uint64) []inflightCompareCandidate {
	candidates := make([]inflightCompareCandidate, 0, len(c.inflight))
	for _, reservation := range c.inflight {
		if !reservation.hasPHash {
			continue
		}
		distance := hasher.HammingDistance(phash, reservation.phash)
		if distance > dedupe.CandidateSearchDistance {
			continue
		}

		comparePath := reservation.task.SourcePath
		compareMeta := reservation.sourceMeta
		compareSize := reservation.task.Size
		compareMMH3 := reservation.task.MMH3Hash
		compareDHash := reservation.phash
		compareHasDHash := reservation.hasPHash

		if reservation.action == importPlanCopyThumbnail && reservation.committedMatchPath != "" {
			comparePath = reservation.committedMatchPath
			compareMeta = ""
			compareSize = 0
			compareMMH3 = ""
			compareDHash = 0
			compareHasDHash = false
		}

		candidates = append(candidates, inflightCompareCandidate{
			done:            reservation.done,
			comparePath:     comparePath,
			compareMetaJSON: compareMeta,
			compareSize:     compareSize,
			compareMMH3:     compareMMH3,
			compareDHash:    compareDHash,
			compareHasDHash: compareHasDHash,
			distance:        distance,
		})
	}
	return candidates
}

func (c *importCoordinator) findInflightVisualMatch(ctx context.Context, task ImportTask, sourceMeta string, phash uint64, candidates []inflightCompareCandidate) <-chan struct{} {
	if len(candidates) == 0 {
		return nil
	}

	childFeatures := resolveImportFeatures(ctx, c.featureResolver, task.MMH3Hash, phash, true, task.SourcePath)

	for idx := range candidates {
		if candidates[idx].compareMetaJSON == "" || candidates[idx].compareSize == 0 || candidates[idx].compareMMH3 == "" || !candidates[idx].compareHasDHash {
			stat, err := os.Stat(candidates[idx].comparePath)
			if err != nil {
				continue
			}
			candidates[idx].compareSize = stat.Size()
			candidates[idx].compareMetaJSON = metadata.ExtractImageMetaJson(candidates[idx].comparePath)

			info, ok := c.cacheManager.GetCachedInfo(candidates[idx].comparePath)
			if ok {
				candidates[idx].compareMMH3 = info.MMH3
				if info.PHash != "" {
					if parsed, parseErr := hasher.StringToPHash(info.PHash); parseErr == nil {
						candidates[idx].compareDHash = parsed
						candidates[idx].compareHasDHash = true
					}
				}
			}
			if candidates[idx].compareMMH3 == "" {
				if computed, computeErr := hasher.CalculateHash(candidates[idx].comparePath); computeErr == nil {
					candidates[idx].compareMMH3 = computed
				}
			}
			if !candidates[idx].compareHasDHash {
				if computed, computeErr := hasher.CalculatePHash(candidates[idx].comparePath); computeErr == nil {
					candidates[idx].compareDHash = computed
					candidates[idx].compareHasDHash = true
				}
			}
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].distance != candidates[j].distance {
			return candidates[i].distance < candidates[j].distance
		}
		if candidates[i].compareSize != candidates[j].compareSize {
			return candidates[i].compareSize > candidates[j].compareSize
		}
		return candidates[i].comparePath < candidates[j].comparePath
	})

	for _, candidate := range candidates {
		if candidate.comparePath == "" || candidate.compareMetaJSON == "" || candidate.compareSize == 0 {
			continue
		}

		parentFeatures := resolveImportFeatures(ctx, c.featureResolver, candidate.compareMMH3, candidate.compareDHash, candidate.compareHasDHash, candidate.comparePath)
		decision, err := dedupe.ClassifyDerivativeWithResolvedFeatures(task.SourcePath, sourceMeta, task.Size, childFeatures, candidate.comparePath, candidate.compareMetaJSON, candidate.compareSize, parentFeatures)
		if err != nil {
			log.Printf("Failed to confirm in-flight visual duplicate [%s] against [%s]: %v", task.SourcePath, candidate.comparePath, err)
			continue
		}
		if decision.Confirmed {
			return candidate.done
		}
	}

	return nil
}
