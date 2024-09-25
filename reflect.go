package timeline

import "reflect"

func typeImplementsInterface[ValType any, Interface any]() bool {
	typeT := typ[ValType]()
	intT := typ[Interface]()
	return typeT.Implements(intT)
}

func typ[T any]() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}
