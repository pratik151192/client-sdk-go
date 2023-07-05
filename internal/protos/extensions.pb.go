// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.23.3
// source: extensions.proto

package client_sdk_go

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// A hint so you can decide a little more in the abstract "can this be retried?""
type RetrySemantic int32

const (
	// Never retry this message without telling the user. (you should infer this as the default)
	RetrySemantic_NotRetryable RetrySemantic = 0
	// You can retry this without surfacing an error to the user.
	RetrySemantic_Retryable RetrySemantic = 1
)

// Enum value maps for RetrySemantic.
var (
	RetrySemantic_name = map[int32]string{
		0: "NotRetryable",
		1: "Retryable",
	}
	RetrySemantic_value = map[string]int32{
		"NotRetryable": 0,
		"Retryable":    1,
	}
)

func (x RetrySemantic) Enum() *RetrySemantic {
	p := new(RetrySemantic)
	*p = x
	return p
}

func (x RetrySemantic) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (RetrySemantic) Descriptor() protoreflect.EnumDescriptor {
	return file_extensions_proto_enumTypes[0].Descriptor()
}

func (RetrySemantic) Type() protoreflect.EnumType {
	return &file_extensions_proto_enumTypes[0]
}

func (x RetrySemantic) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use RetrySemantic.Descriptor instead.
func (RetrySemantic) EnumDescriptor() ([]byte, []int) {
	return file_extensions_proto_rawDescGZIP(), []int{0}
}

var file_extensions_proto_extTypes = []protoimpl.ExtensionInfo{
	{
		ExtendedType:  (*descriptorpb.MessageOptions)(nil),
		ExtensionType: (*RetrySemantic)(nil),
		Field:         50000,
		Name:          "retry_semantic",
		Tag:           "varint,50000,opt,name=retry_semantic,enum=RetrySemantic",
		Filename:      "extensions.proto",
	},
}

// Extension fields to descriptorpb.MessageOptions.
var (
	// Can this message be re-driven without an error?
	//
	// optional RetrySemantic retry_semantic = 50000;
	E_RetrySemantic = &file_extensions_proto_extTypes[0]
)

var File_extensions_proto protoreflect.FileDescriptor

var file_extensions_proto_rawDesc = []byte{
	0x0a, 0x10, 0x65, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x20, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2f, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x6f, 0x72, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2a, 0x30, 0x0a, 0x0d, 0x52, 0x65, 0x74, 0x72, 0x79, 0x53, 0x65, 0x6d,
	0x61, 0x6e, 0x74, 0x69, 0x63, 0x12, 0x10, 0x0a, 0x0c, 0x4e, 0x6f, 0x74, 0x52, 0x65, 0x74, 0x72,
	0x79, 0x61, 0x62, 0x6c, 0x65, 0x10, 0x00, 0x12, 0x0d, 0x0a, 0x09, 0x52, 0x65, 0x74, 0x72, 0x79,
	0x61, 0x62, 0x6c, 0x65, 0x10, 0x01, 0x3a, 0x58, 0x0a, 0x0e, 0x72, 0x65, 0x74, 0x72, 0x79, 0x5f,
	0x73, 0x65, 0x6d, 0x61, 0x6e, 0x74, 0x69, 0x63, 0x12, 0x1f, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x4d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xd0, 0x86, 0x03, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x0e, 0x2e, 0x52, 0x65, 0x74, 0x72, 0x79, 0x53, 0x65, 0x6d, 0x61, 0x6e, 0x74, 0x69,
	0x63, 0x52, 0x0d, 0x72, 0x65, 0x74, 0x72, 0x79, 0x53, 0x65, 0x6d, 0x61, 0x6e, 0x74, 0x69, 0x63,
	0x42, 0x5f, 0x0a, 0x0f, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x65, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69,
	0x6f, 0x6e, 0x73, 0x5a, 0x30, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x6d, 0x6f, 0x6d, 0x65, 0x6e, 0x74, 0x6f, 0x68, 0x71, 0x2f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74,
	0x2d, 0x73, 0x64, 0x6b, 0x2d, 0x67, 0x6f, 0x3b, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x73,
	0x64, 0x6b, 0x5f, 0x67, 0x6f, 0xaa, 0x02, 0x19, 0x4d, 0x6f, 0x6d, 0x65, 0x6e, 0x74, 0x6f, 0x2e,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2e, 0x45, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e,
	0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_extensions_proto_rawDescOnce sync.Once
	file_extensions_proto_rawDescData = file_extensions_proto_rawDesc
)

func file_extensions_proto_rawDescGZIP() []byte {
	file_extensions_proto_rawDescOnce.Do(func() {
		file_extensions_proto_rawDescData = protoimpl.X.CompressGZIP(file_extensions_proto_rawDescData)
	})
	return file_extensions_proto_rawDescData
}

var file_extensions_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_extensions_proto_goTypes = []interface{}{
	(RetrySemantic)(0),                  // 0: RetrySemantic
	(*descriptorpb.MessageOptions)(nil), // 1: google.protobuf.MessageOptions
}
var file_extensions_proto_depIdxs = []int32{
	1, // 0: retry_semantic:extendee -> google.protobuf.MessageOptions
	0, // 1: retry_semantic:type_name -> RetrySemantic
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	1, // [1:2] is the sub-list for extension type_name
	0, // [0:1] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_extensions_proto_init() }
func file_extensions_proto_init() {
	if File_extensions_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_extensions_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   0,
			NumExtensions: 1,
			NumServices:   0,
		},
		GoTypes:           file_extensions_proto_goTypes,
		DependencyIndexes: file_extensions_proto_depIdxs,
		EnumInfos:         file_extensions_proto_enumTypes,
		ExtensionInfos:    file_extensions_proto_extTypes,
	}.Build()
	File_extensions_proto = out.File
	file_extensions_proto_rawDesc = nil
	file_extensions_proto_goTypes = nil
	file_extensions_proto_depIdxs = nil
}
