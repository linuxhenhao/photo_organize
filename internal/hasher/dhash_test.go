package hasher

import "testing"

func TestDHashStringRoundTrip(t *testing.T) {
	tests := []struct {
		val    uint64
		strVal string
	}{
		{val: 0x0, strVal: "0000000000000000"},
		{val: 0x1, strVal: "0000000000000001"},
		{val: 0xdeadbeef, strVal: "00000000deadbeef"},
		{val: 0x0123456789abcdef, strVal: "0123456789abcdef"},
	}

	for _, tt := range tests {
		s := DHashToString(tt.val)
		if s != tt.strVal {
			t.Errorf("DHashToString(%x) = %s; want %s", tt.val, s, tt.strVal)
		}
		v, err := StringToDHash(tt.strVal)
		if err != nil {
			t.Errorf("StringToDHash(%s) error: %v", tt.strVal, err)
			continue
		}
		if v != tt.val {
			t.Errorf("StringToDHash(%s) = %x; want %x", tt.strVal, v, tt.val)
		}
	}
}
