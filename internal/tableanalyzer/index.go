package tableanalyzer

import "slices"

type (
	Index struct {
		keyName     string
		indexType   string
		seq         int64
		column      string
		cardinality int64
	}
	CompositeIndexes map[string][]Index
	CompositeIndex   []Index
)

func findCompositeIndexes(indexes []Index) (CompositeIndexes, error) {
	hmap := make(CompositeIndexes)
	for _, idx := range indexes {
		hmap[idx.keyName] = append(hmap[idx.keyName], idx)
	}
	for k, v := range hmap {
		if len(v) == 1 {
			delete(hmap, k)
		}
	}
	for k := range hmap {
		slices.SortFunc(hmap[k], func(a, b Index) int {
			return int(a.seq) - int(b.seq)
		})
	}
	return hmap, nil
}

// checkCardinality checks if columns in a composite index are ordered based on their cardinality
// If it's not ordered well, the function returns the optimal index in the right order
func checkCardinality(compIdx CompositeIndex) (optimalIndex CompositeIndex, ok bool) {
	optimalIdx := make([]Index, len(compIdx))
	copy(optimalIdx, compIdx)

	slices.SortFunc(optimalIdx, func(a, b Index) int {
		return int(a.cardinality) - int(b.cardinality)
	})

	for i, v := range optimalIdx {
		if compIdx[i] != v {
			return optimalIdx, false
		}
	}
	return nil, true
}
