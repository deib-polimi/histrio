package utils

// comparator returns 1 if v1 < v2
func findMin[K comparable, V any](myMap map[K]V, comparator func(v1 V, v2 V) int) (K, V, bool) {
	var minKey K
	var minVal V
	if len(myMap) == 0 {
		return minKey, minVal, false
	}

	firstIteration := true
	for key, val := range myMap {
		if firstIteration {
			minKey = key
			minVal = val
			firstIteration = false
		} else {
			if comparator(val, minVal) == 1 {
				minKey = key
				minVal = val
			}
		}
	}

	return minKey, minVal, true

}
