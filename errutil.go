package timeline

import "fmt"

func assert(condition bool, format string, args ...interface{}) {
	if !condition {
		panic(fmt.Errorf(format, args...))
	}
}
