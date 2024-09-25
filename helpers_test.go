package timeline_test

func Map[OldElem any, NewElem any](arr []OldElem, transform func(item OldElem, index int) NewElem) []NewElem {
	res := make([]NewElem, len(arr))

	for i, oldElem := range arr {
		res[i] = transform(oldElem, i)
	}

	return res
}
