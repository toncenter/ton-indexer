package protocol

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// UnknownField tells where a message holds a field this build does not know,
// "" if nowhere. A newer SDK may send a kind of call or a field that an older
// fireton or router lacks. Answering it with INVALID_REQUEST would be cached,
// and would differ from the answer of a newer server to the same module, so
// such a request is an error of infrastructure instead.
func UnknownField(m proto.Message) string {
	r := m.ProtoReflect()
	return unknownIn(r, string(r.Descriptor().Name()))
}

func unknownIn(m protoreflect.Message, path string) string {
	if len(m.GetUnknown()) > 0 {
		return path
	}
	found := ""
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		at := path + "." + string(fd.Name())
		switch {
		case fd.IsMap():
			if fd.MapValue().Message() != nil {
				v.Map().Range(func(k protoreflect.MapKey, value protoreflect.Value) bool {
					found = unknownIn(value.Message(), fmt.Sprintf("%s[%v]", at, k.Interface()))
					return found == ""
				})
			}
		case fd.IsList():
			if fd.Message() != nil {
				for i := 0; i < v.List().Len() && found == ""; i++ {
					found = unknownIn(v.List().Get(i).Message(), fmt.Sprintf("%s[%d]", at, i))
				}
			}
		case fd.Message() != nil:
			found = unknownIn(v.Message(), at)
		}
		return found == ""
	})
	return found
}
