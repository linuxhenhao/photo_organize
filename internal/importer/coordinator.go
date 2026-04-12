package importer

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/linuxhenhao/photo_organize/internal/dedupe"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/linuxhenhao/photo_organize/internal/target"
)

type importPlanAction int

const (
	importPlanSkip importPlanAction = iota
	importPlanWait
	importPlanCopyMaster
	importPlanCopyThumbnail
	importPlanPromoteCommitted
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

type inflightVisualCandidate struct {
	reservation *importReservation
	distance    int
}

type confirmedImportMatch struct {
	match           hasher.MatchResult
	preferCandidate bool
}

type importCoordinator struct {
	cacheManager *target.CacheManager

	mutex          sync.Mutex
	nextSeq        uint64
	inflight       map[uint64]*importReservation
	inflightByMMH3 map[string]*importReservation
	inflightByPath map[string]*importReservation
}

func newImportCoordinator(cacheManager *target.CacheManager) *importCoordinator {
	return &importCoordinator{
		cacheManager:   cacheManager,
		inflight:       make(map[uint64]*importReservation),
		inflightByMMH3: make(map[string]*importReservation),
		inflightByPath: make(map[string]*importReservation),
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

func (c *importCoordinator) planTask(task ImportTask, sourceMeta string) importPlan {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, found := c.findCommittedExactMatchLocked(task.MMH3Hash); found {
		return importPlan{action: importPlanSkip}
	}

	if task.MMH3Hash != "" {
		if _, found := c.inflightByMMH3[task.MMH3Hash]; found {
			return importPlan{action: importPlanSkip}
		}
	}

	phash, hasPHash := parseTaskPHash(task)
	if hasPHash {
		if waitCh := c.findInflightVisualMatchLocked(task, sourceMeta, phash); waitCh != nil {
			return importPlan{action: importPlanWait, waitCh: waitCh}
		}
	}

	masterTargetPath := c.resolveAvailableTargetPathLocked(task.TargetDir, task.FileName)
	if hasPHash {
		if match := c.findCommittedVisualMatchLocked(task, sourceMeta, phash); match != nil {
			if match.preferCandidate {
				reservation := c.reserveLocked(task, sourceMeta, masterTargetPath, phash, true, importPlanPromoteCommitted, match.match.Path)
				return importPlan{action: importPlanPromoteCommitted, reservation: reservation}
			}

			thumbDir := filepath.Join(
				targetDirRoot(task.TargetDir),
				"thumbnails",
				filepath.Base(filepath.Dir(filepath.Dir(task.TargetDir))),
				filepath.Base(filepath.Dir(task.TargetDir)),
				filepath.Base(task.TargetDir),
			)
			thumbTargetPath := c.resolveAvailableTargetPathLocked(thumbDir, task.FileName)
			reservation := c.reserveLocked(task, sourceMeta, thumbTargetPath, phash, true, importPlanCopyThumbnail, match.match.Path)
			return importPlan{action: importPlanCopyThumbnail, reservation: reservation}
		}
	}

	reservation := c.reserveLocked(task, sourceMeta, masterTargetPath, phash, hasPHash, importPlanCopyMaster, "")
	return importPlan{action: importPlanCopyMaster, reservation: reservation}
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

func (c *importCoordinator) commitReservation(reservation *importReservation) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch reservation.action {
	case importPlanCopyMaster:
		c.cacheManager.AddEntryWithPresence(
			reservation.finalPath,
			reservation.task.MMH3Hash,
			reservation.phash,
			reservation.hasPHash,
			reservation.task.Size,
			reservation.sourceMeta,
		)
	case importPlanCopyThumbnail:
		if c.isCommittedPathValidLocked(reservation.committedMatchPath) {
			c.cacheManager.AppendThumbnailToMaster(reservation.committedMatchPath, reservation.finalPath, reservation.sourceMeta)
		} else {
			recoveryPath := c.resolveAvailableTargetPathLocked(reservation.task.TargetDir, reservation.task.FileName)
			if recoveryPath != reservation.finalPath {
				if err := os.MkdirAll(filepath.Dir(recoveryPath), 0755); err == nil {
					if err := os.Rename(reservation.finalPath, recoveryPath); err == nil {
						reservation.finalPath = recoveryPath
					} else {
						log.Printf("Failed to recover thumbnail [%s] to master path [%s]: %v", reservation.finalPath, recoveryPath, err)
					}
				}
			}
			c.cacheManager.AddEntryWithPresence(
				reservation.finalPath,
				reservation.task.MMH3Hash,
				reservation.phash,
				reservation.hasPHash,
				reservation.task.Size,
				reservation.sourceMeta,
			)
		}
	case importPlanPromoteCommitted:
		c.cacheManager.AddEntryWithPresence(
			reservation.finalPath,
			reservation.task.MMH3Hash,
			reservation.phash,
			reservation.hasPHash,
			reservation.task.Size,
			reservation.sourceMeta,
		)

		if c.isCommittedPathValidLocked(reservation.committedMatchPath) {
			baseDir := targetDirRoot(filepath.Dir(reservation.finalPath))
			thumbPath := moveFileToThumbnails(baseDir, reservation.committedMatchPath)
			if thumbPath == reservation.committedMatchPath {
				log.Printf("Keeping both files because old master [%s] could not be moved to thumbnails", reservation.committedMatchPath)
			} else {
				thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
				c.cacheManager.DeleteEntry(reservation.committedMatchPath)
				c.cacheManager.AppendThumbnailToMaster(reservation.finalPath, thumbPath, thumbMeta)
			}
		}
	}

	c.removeReservationLocked(reservation)
	close(reservation.done)
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

func (c *importCoordinator) findCommittedVisualMatchLocked(task ImportTask, sourceMeta string, phash uint64) *confirmedImportMatch {
	matches := c.cacheManager.SearchPHash(phash, dedupe.CandidateSearchDistance)
	for _, candidate := range matches {
		if _, err := os.Stat(candidate.Path); err != nil {
			log.Printf("Removing stale perceptual match entry for missing path [%s]", candidate.Path)
			c.cacheManager.DeleteEntry(candidate.Path)
			continue
		}

		existingMeta := metadata.ExtractImageMetaJson(candidate.Path)
		decision, err := dedupe.EvaluateThumbnailMatch(task.SourcePath, sourceMeta, task.Size, candidate.Path, existingMeta, candidate.Size)
		if err != nil {
			log.Printf("Failed to confirm visual duplicate [%s] against [%s]: %v", task.SourcePath, candidate.Path, err)
			continue
		}
		if decision.Confirmed {
			return &confirmedImportMatch{
				match:           candidate,
				preferCandidate: decision.PreferCandidate,
			}
		}
	}

	return nil
}

func (c *importCoordinator) findInflightVisualMatchLocked(task ImportTask, sourceMeta string, phash uint64) <-chan struct{} {
	candidates := make([]inflightVisualCandidate, 0, len(c.inflight))
	for _, reservation := range c.inflight {
		if !reservation.hasPHash {
			continue
		}
		distance := hasher.HammingDistance(phash, reservation.phash)
		if distance > dedupe.CandidateSearchDistance {
			continue
		}
		candidates = append(candidates, inflightVisualCandidate{
			reservation: reservation,
			distance:    distance,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].distance != candidates[j].distance {
			return candidates[i].distance < candidates[j].distance
		}
		if candidates[i].reservation.task.Size != candidates[j].reservation.task.Size {
			return candidates[i].reservation.task.Size > candidates[j].reservation.task.Size
		}
		return candidates[i].reservation.finalPath < candidates[j].reservation.finalPath
	})

	for _, candidate := range candidates {
		comparePath, compareMeta, ok := c.comparePathForReservationLocked(candidate.reservation)
		if !ok {
			continue
		}

		decision, err := dedupe.EvaluateThumbnailMatch(
			task.SourcePath,
			sourceMeta,
			task.Size,
			comparePath,
			compareMeta,
			candidate.reservation.task.Size,
		)
		if err != nil {
			log.Printf("Failed to confirm in-flight visual duplicate [%s] against [%s]: %v", task.SourcePath, comparePath, err)
			continue
		}
		if decision.Confirmed {
			return candidate.reservation.done
		}
	}

	return nil
}

func (c *importCoordinator) comparePathForReservationLocked(reservation *importReservation) (string, string, bool) {
	if _, err := os.Stat(reservation.task.SourcePath); err == nil {
		return reservation.task.SourcePath, reservation.sourceMeta, true
	}

	if _, err := os.Stat(reservation.finalPath); err == nil {
		return reservation.finalPath, metadata.ExtractImageMetaJson(reservation.finalPath), true
	}

	return "", "", false
}
