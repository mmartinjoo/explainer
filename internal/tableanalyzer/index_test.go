package tableanalyzer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFindCompositeIndexes(t *testing.T) {
	compIdxCol1 := Index{
		keyName:     "comp_idx",
		indexType:   "BTREE",
		seq:         1,
		column:      "c1",
		cardinality: 10,
	}
	compIdxCol2 := Index{
		keyName:     "comp_idx",
		indexType:   "BTREE",
		seq:         2,
		column:      "c2",
		cardinality: 20,
	}

	compIdx2Col1 := Index{
		keyName:     "comp_idx2",
		indexType:   "BTREE",
		seq:         1,
		column:      "c10",
		cardinality: 10,
	}
	compIdx2Col2 := Index{
		keyName:     "comp_idx2",
		indexType:   "BTREE",
		seq:         2,
		column:      "c11",
		cardinality: 20,
	}

	standardIdx := Index{
		keyName:     "idx",
		indexType:   "BTREE",
		seq:         1,
		column:      "c5",
		cardinality: 30,
	}

	compIndexes, err := findCompositeIndexes([]Index{compIdxCol1, compIdxCol2, standardIdx, compIdx2Col1, compIdx2Col2})
	assert.Nil(t, err)
	assert.Len(t, compIndexes, 2)
	assert.Equal(t, []Index{compIdxCol1, compIdxCol2}, compIndexes["comp_idx"])
	assert.Equal(t, []Index{compIdx2Col1, compIdx2Col2}, compIndexes["comp_idx2"])
}

func TestCheckCardinality_Ok(t *testing.T) {
	compIdxCol1 := Index{
		keyName:     "comp_idx",
		indexType:   "BTREE",
		seq:         1,
		column:      "c1",
		cardinality: 10,
	}
	compIdxCol2 := Index{
		keyName:     "comp_idx",
		indexType:   "BTREE",
		seq:         2,
		column:      "c2",
		cardinality: 20,
	}
	compIdxCol3 := Index{
		keyName:     "comp_idx",
		indexType:   "BTREE",
		seq:         3,
		column:      "c3",
		cardinality: 30,
	}

	_, ok := checkCardinality([]Index{compIdxCol1, compIdxCol2, compIdxCol3})
	assert.True(t, ok)
}

func TestCheckCardinality_NotOk(t *testing.T) {
	compIdxCol1 := Index{
		keyName:     "comp_idx",
		indexType:   "BTREE",
		seq:         1,
		column:      "c1",
		cardinality: 10,
	}
	compIdxCol2 := Index{
		keyName:     "comp_idx",
		indexType:   "BTREE",
		seq:         2,
		column:      "c3",
		cardinality: 30,
	}
	compIdxCol3 := Index{
		keyName:     "comp_idx",
		indexType:   "BTREE",
		seq:         3,
		column:      "c2",
		cardinality: 20,
	}

	optimalIdx, ok := checkCardinality([]Index{compIdxCol1, compIdxCol2, compIdxCol3})
	assert.False(t, ok)

	assert.Equal(t, "c1", optimalIdx[0].column)
	assert.Equal(t, "c2", optimalIdx[1].column)
	assert.Equal(t, "c3", optimalIdx[2].column)
}
