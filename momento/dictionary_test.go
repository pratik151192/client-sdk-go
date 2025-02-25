package momento_test

import (
	"time"

	. "github.com/momentohq/client-sdk-go/momento"
	. "github.com/momentohq/client-sdk-go/momento/test_helpers"
	. "github.com/momentohq/client-sdk-go/responses"
	. "github.com/momentohq/client-sdk-go/utils"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Dictionary methods", func() {
	var sharedContext SharedContext

	BeforeEach(func() {
		sharedContext = NewSharedContext()
		sharedContext.CreateDefaultCaches()
		DeferCleanup(func() {
			sharedContext.Close()
		})
	})

	DescribeTable("try using invalid cache and dictionary names",
		func(cacheName string, dictionaryName string, expectedErrorCode string) {
			Expect(
				sharedContext.Client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
					CacheName:      cacheName,
					DictionaryName: dictionaryName,
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				sharedContext.Client.DictionaryIncrement(sharedContext.Ctx, &DictionaryIncrementRequest{
					CacheName:      cacheName,
					DictionaryName: dictionaryName,
					Field:          String("hi"),
					Amount:         1,
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				sharedContext.Client.DictionaryRemoveField(sharedContext.Ctx, &DictionaryRemoveFieldRequest{
					CacheName:      cacheName,
					DictionaryName: dictionaryName,
					Field:          String("hi"),
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				sharedContext.Client.DictionaryRemoveFields(sharedContext.Ctx, &DictionaryRemoveFieldsRequest{
					CacheName:      cacheName,
					DictionaryName: dictionaryName,
					Fields:         []Value{String("hi")},
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				sharedContext.Client.DictionaryGetFields(sharedContext.Ctx, &DictionaryGetFieldsRequest{
					CacheName:      cacheName,
					DictionaryName: dictionaryName,
					Fields:         []Value{String("hi")},
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				sharedContext.Client.DictionaryGetField(sharedContext.Ctx, &DictionaryGetFieldRequest{
					CacheName:      cacheName,
					DictionaryName: dictionaryName,
					Field:          String("hi"),
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				sharedContext.Client.DictionarySetField(sharedContext.Ctx, &DictionarySetFieldRequest{
					CacheName:      cacheName,
					DictionaryName: dictionaryName,
					Field:          String("hi"),
					Value:          String("hi"),
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				sharedContext.Client.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
					CacheName:      cacheName,
					DictionaryName: dictionaryName,
					Elements:       nil,
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))
		},
		Entry("nonexistent cache name", uuid.NewString(), uuid.NewString(), NotFoundError),
		Entry("empty cache name", "", sharedContext.CollectionName, InvalidArgumentError),
		Entry("empty dictionary name", sharedContext.CacheName, "", InvalidArgumentError),
		Entry("nil dictionary name", sharedContext.CacheName, nil, InvalidArgumentError),
		Entry("nil cache name", nil, sharedContext.CollectionName, InvalidArgumentError),
	)

	DescribeTable("add string and bytes value for single field happy path",
		func(clientType string, field Value, value Value, expectedFieldString string, expectedFieldBytes []byte, expectedValueString string, expectedValueBytes []byte) {
			client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
			Expect(
				client.DictionarySetField(sharedContext.Ctx, &DictionarySetFieldRequest{
					CacheName:      cacheName,
					DictionaryName: sharedContext.CollectionName,
					Field:          field,
					Value:          value,
				}),
			).Error().To(BeNil())
			getFieldResp, err := client.DictionaryGetField(sharedContext.Ctx, &DictionaryGetFieldRequest{
				CacheName:      cacheName,
				DictionaryName: sharedContext.CollectionName,
				Field:          field,
			})
			Expect(err).To(BeNil())
			Expect(getFieldResp).To(BeAssignableToTypeOf(&DictionaryGetFieldHit{}))
			switch result := getFieldResp.(type) {
			case *DictionaryGetFieldHit:
				Expect(result.FieldString()).To(Equal(expectedFieldString))
				Expect(result.FieldByte()).To(Equal(expectedFieldBytes))
				Expect(result.ValueString()).To(Equal(expectedValueString))
				Expect(result.ValueByte()).To(Equal(expectedValueBytes))
			}
		},
		Entry("using string value and field", DefaultClient, String("myField"), String("myValue"), "myField", []byte("myField"), "myValue", []byte("myValue")),
		Entry("using string value and bytes field", DefaultClient, String("myField"), Bytes("myValue"), "myField", []byte("myField"), "myValue", []byte("myValue")),
		Entry("using bytes value and string field", DefaultClient, Bytes("myField"), String("myValue"), "myField", []byte("myField"), "myValue", []byte("myValue")),
		Entry("using bytes value and field", DefaultClient, Bytes("myField"), Bytes("myValue"), "myField", []byte("myField"), "myValue", []byte("myValue")),
		Entry("using string value and field with default cache", WithDefaultCache, String("myField"), String("myValue"), "myField", []byte("myField"), "myValue", []byte("myValue")),
		Entry("using string value and bytes field with default cache", WithDefaultCache, String("myField"), Bytes("myValue"), "myField", []byte("myField"), "myValue", []byte("myValue")),
		Entry("using bytes value and string field with default cache", WithDefaultCache, Bytes("myField"), String("myValue"), "myField", []byte("myField"), "myValue", []byte("myValue")),
		Entry("using bytes value and field with default cache", WithDefaultCache, Bytes("myField"), Bytes("myValue"), "myField", []byte("myField"), "myValue", []byte("myValue")),
	)

	DescribeTable("try using empty and nil fields and values for set",
		func(field Value, value Value) {
			Expect(
				sharedContext.Client.DictionarySetField(sharedContext.Ctx, &DictionarySetFieldRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Field:          field,
					Value:          value,
				}),
			).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
		},
		Entry("empty field", String(""), String("value")),
		Entry("nil field", nil, String("value")),
		Entry("nil value", String("field"), nil),
		Entry("both nil", nil, nil),
	)

	It("errors with a negative ttl for set", func() {
		Expect(
			sharedContext.Client.DictionarySetField(sharedContext.Ctx, &DictionarySetFieldRequest{
				CacheName:      sharedContext.CacheName,
				DictionaryName: sharedContext.CollectionName,
				Field:          String("myField"),
				Value:          String("myValue"),
				Ttl:            &CollectionTtl{Ttl: time.Duration(-1), RefreshTtl: true},
			}),
		).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
	})

	DescribeTable("add string fields and string and bytes values for set fields happy path",
		func(clientType string, elements []DictionaryElement, expectedItemsStringValue map[string]string, expectedItemsByteValue map[string][]byte) {
			client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
			Expect(
				client.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
					CacheName:      cacheName,
					DictionaryName: sharedContext.CollectionName,
					Elements:       elements,
				}),
			).To(BeAssignableToTypeOf(&DictionarySetFieldsSuccess{}))
			fetchResp, err := client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
				CacheName:      cacheName,
				DictionaryName: sharedContext.CollectionName,
			})
			Expect(err).To(BeNil())
			switch result := fetchResp.(type) {
			case *DictionaryFetchMiss:
				Fail("got a miss for a dictionary fetch that should have been a hit")
			case *DictionaryFetchHit:
				i := 0
				keys := make([]string, len(result.ValueMap()))
				for k := range result.ValueMap() {
					keys[i] = k
					i++
				}
				Expect(len(result.ValueMap())).To(Equal(len(expectedItemsStringValue)))
				Expect(len(result.ValueMapStringString())).To(Equal(len(expectedItemsStringValue)))
				Expect(len(result.ValueMapStringByte())).To(Equal(len(expectedItemsByteValue)))
				for k, v := range result.ValueMap() {
					Expect(expectedItemsStringValue[k]).To(Equal(v))
				}
				for k, v := range result.ValueMapStringString() {
					Expect(expectedItemsStringValue[k]).To(Equal(v))
				}
				for k, v := range result.ValueMapStringByte() {
					Expect(expectedItemsByteValue[k]).To(Equal(v))
				}
			}
		},
		Entry(
			"with string values",
			DefaultClient,
			[]DictionaryElement{
				{Field: String("myField1"), Value: String("myValue1")},
				{Field: String("myField2"), Value: String("myValue2")},
			},
			map[string]string{"myField1": "myValue1", "myField2": "myValue2"},
			map[string][]byte{"myField1": []byte("myValue1"), "myField2": []byte("myValue2")},
		),
		Entry(
			"with byte values",
			DefaultClient,
			[]DictionaryElement{
				{Field: Bytes("myField1"), Value: Bytes("myValue1")},
				{Field: Bytes("myField2"), Value: Bytes("myValue2")},
			},
			map[string]string{"myField1": "myValue1", "myField2": "myValue2"},
			map[string][]byte{"myField1": []byte("myValue1"), "myField2": []byte("myValue2")},
		),
		Entry(
			"with mixed values",
			DefaultClient,
			[]DictionaryElement{
				{Field: Bytes("myField1"), Value: String("myValue1")},
				{Field: String("myField2"), Value: Bytes("myValue2")},
			},
			map[string]string{"myField1": "myValue1", "myField2": "myValue2"},
			map[string][]byte{"myField1": []byte("myValue1"), "myField2": []byte("myValue2")},
		),
		Entry(
			"with empty values",
			DefaultClient,
			[]DictionaryElement{
				{Field: Bytes("myField1"), Value: String("")},
				{Field: String("myField2"), Value: Bytes("")},
			},
			map[string]string{"myField1": "", "myField2": ""},
			map[string][]byte{"myField1": []byte(""), "myField2": []byte("")},
		),
		Entry(
			"with string values and default cache",
			WithDefaultCache,
			[]DictionaryElement{
				{Field: String("myField1"), Value: String("myValue1")},
				{Field: String("myField2"), Value: String("myValue2")},
			},
			map[string]string{"myField1": "myValue1", "myField2": "myValue2"},
			map[string][]byte{"myField1": []byte("myValue1"), "myField2": []byte("myValue2")},
		),
		Entry(
			"with byte values and default cache",
			WithDefaultCache,
			[]DictionaryElement{
				{Field: Bytes("myField1"), Value: Bytes("myValue1")},
				{Field: Bytes("myField2"), Value: Bytes("myValue2")},
			},
			map[string]string{"myField1": "myValue1", "myField2": "myValue2"},
			map[string][]byte{"myField1": []byte("myValue1"), "myField2": []byte("myValue2")},
		),
		Entry(
			"with mixed values and default cache",
			WithDefaultCache,
			[]DictionaryElement{
				{Field: Bytes("myField1"), Value: String("myValue1")},
				{Field: String("myField2"), Value: Bytes("myValue2")},
			},
			map[string]string{"myField1": "myValue1", "myField2": "myValue2"},
			map[string][]byte{"myField1": []byte("myValue1"), "myField2": []byte("myValue2")},
		),
		Entry(
			"with empty values and default cache",
			WithDefaultCache,
			[]DictionaryElement{
				{Field: Bytes("myField1"), Value: String("")},
				{Field: String("myField2"), Value: Bytes("")},
			},
			map[string]string{"myField1": "", "myField2": ""},
			map[string][]byte{"myField1": []byte(""), "myField2": []byte("")},
		),
	)

	It("returns an error if an item field is empty", func() {
		Expect(
			sharedContext.Client.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
				CacheName:      sharedContext.CacheName,
				DictionaryName: sharedContext.CollectionName,
				Elements: []DictionaryElement{
					{Field: String("myField"), Value: String("myValue")},
					{Field: String(""), Value: String("myOtherValue")},
				},
			}),
		)
	})

	It("returns an error if an item value is nil", func() {
		Expect(
			sharedContext.Client.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
				CacheName:      sharedContext.CacheName,
				DictionaryName: sharedContext.CollectionName,
				Elements: []DictionaryElement{
					{Field: String("myField"), Value: String("myValue")},
					{Field: String("myOtherField"), Value: nil},
				},
			}),
		).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
	})

	Describe("map to elements conversion functions", func() {

		It("converts from a map with string values to element slice", func() {
			theMap := map[string]string{"myField1": "myValue1", "myField2": "myValue2"}
			expected := []DictionaryElement{
				{Field: String("myField1"), Value: String("myValue1")},
				{Field: String("myField2"), Value: String("myValue2")},
			}
			elems := DictionaryElementsFromMapStringString(theMap)
			Expect(elems).To(ConsistOf(expected))
		})

		It("converts from a map with bytes values to element slice", func() {
			theMap := map[string][]byte{"myField1": []byte("myValue1"), "myField2": []byte("myValue2")}
			expected := []DictionaryElement{
				{Field: String("myField1"), Value: Bytes("myValue1")},
				{Field: String("myField2"), Value: Bytes("myValue2")},
			}
			elems := DictionaryElementsFromMapStringBytes(theMap)
			Expect(elems).To(ConsistOf(expected))
		})

		It("converts from a map with Value values to element slice", func() {
			theMap := map[string]Value{"myField1": String("myValue1"), "myField2": Bytes("myValue2")}
			expected := []DictionaryElement{
				{Field: String("myField1"), Value: String("myValue1")},
				{Field: String("myField2"), Value: Bytes("myValue2")},
			}
			elems := DictionaryElementsFromMapStringValue(theMap)
			Expect(elems).To(ConsistOf(expected))
		})

	})

	Describe("dictionary increment", func() {

		DescribeTable("populates nonexistent field",
			func(clientType string) {
				client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
				incrResp, err := client.DictionaryIncrement(sharedContext.Ctx, &DictionaryIncrementRequest{
					CacheName:      cacheName,
					DictionaryName: sharedContext.CollectionName,
					Field:          String("myField"),
					Amount:         3,
				})
				Expect(err).To(BeNil())
				Expect(incrResp).To(BeAssignableToTypeOf(&DictionaryIncrementSuccess{}))
				switch result := incrResp.(type) {
				case *DictionaryIncrementSuccess:
					Expect(result.Value()).To(Equal(int64(3)))
				}
			},
			Entry("with default client", DefaultClient),
			Entry("with client with default cache", WithDefaultCache),
		)

		It("returns an error when called on a non-integer field", func() {
			Expect(
				sharedContext.Client.DictionarySetField(sharedContext.Ctx, &DictionarySetFieldRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Field:          String("notacounter"),
					Value:          String("notanumber"),
				}),
			).To(BeAssignableToTypeOf(&DictionarySetFieldSuccess{}))

			Expect(
				sharedContext.Client.DictionaryIncrement(sharedContext.Ctx, &DictionaryIncrementRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Field:          String("notacounter"),
					Amount:         1,
				}),
			).Error().To(HaveMomentoErrorCode(FailedPreconditionError))
		})

		It("returns an error when amount is zero", func() {
			_, err := sharedContext.Client.DictionaryIncrement(sharedContext.Ctx, &DictionaryIncrementRequest{
				CacheName:      sharedContext.CacheName,
				DictionaryName: sharedContext.CollectionName,
				Field:          String("myField"),
				Amount:         0,
			})
			Expect(err).To(HaveMomentoErrorCode(InvalidArgumentError))
		})

		DescribeTable("increments on the happy path",
			func(clientType string) {
				client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
				field := String("counter")
				for i := 0; i < 10; i++ {
					Expect(
						client.DictionaryIncrement(sharedContext.Ctx, &DictionaryIncrementRequest{
							CacheName:      cacheName,
							DictionaryName: sharedContext.CollectionName,
							Field:          field,
							Amount:         1,
						}),
					).To(BeAssignableToTypeOf(&DictionaryIncrementSuccess{}))
				}
				fetchResp, err := client.DictionaryGetField(sharedContext.Ctx, &DictionaryGetFieldRequest{
					CacheName:      cacheName,
					DictionaryName: sharedContext.CollectionName,
					Field:          field,
				})
				Expect(err).To(BeNil())
				Expect(fetchResp).To(BeAssignableToTypeOf(&DictionaryGetFieldHit{}))
				switch result := fetchResp.(type) {
				case *DictionaryGetFieldHit:
					Expect(result.ValueString()).To(Equal("10"))
				default:
					Fail("expected a hit for get field but got a miss")
				}
			},
			Entry("with default client", DefaultClient),
			Entry("with client with default cache", WithDefaultCache),
		)

		It("returns an error when field is nil", func() {
			Expect(
				sharedContext.Client.DictionaryIncrement(sharedContext.Ctx, &DictionaryIncrementRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Field:          nil,
					Amount:         1,
				}),
			).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
		})
	})

	Describe("dictionary get", func() {

		BeforeEach(func() {
			Expect(
				sharedContext.Client.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Elements: DictionaryElementsFromMapStringValue(
						map[string]Value{"myField1": String("myValue1"), "myField2": Bytes("myValue2")},
					),
				}),
			).To(BeAssignableToTypeOf(&DictionarySetFieldsSuccess{}))
			Expect(
				sharedContext.ClientWithDefaultCacheName.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
					DictionaryName: sharedContext.CollectionName,
					Elements: DictionaryElementsFromMapStringValue(
						map[string]Value{"myField1": String("myValue1"), "myField2": Bytes("myValue2")},
					),
				}),
			).To(BeAssignableToTypeOf(&DictionarySetFieldsSuccess{}))
		})

		When("getting single field", func() {

			DescribeTable("returns the correct string and byte values",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					expected := map[string]string{"myField1": "myValue1", "myField2": "myValue2"}

					for fieldName, valueStr := range expected {
						getResp, err := client.DictionaryGetField(sharedContext.Ctx, &DictionaryGetFieldRequest{
							CacheName:      cacheName,
							DictionaryName: sharedContext.CollectionName,
							Field:          String(fieldName),
						})
						Expect(err).To(BeNil())
						Expect(getResp).To(BeAssignableToTypeOf(&DictionaryGetFieldHit{}))
						switch result := getResp.(type) {
						case *DictionaryGetFieldHit:
							Expect(result.FieldString()).To(Equal(fieldName))
							Expect(result.FieldByte()).To(Equal([]byte(fieldName)))
							Expect(result.ValueString()).To(Equal(valueStr))
							Expect(result.ValueByte()).To(Equal([]byte(valueStr)))
						default:
							Fail("something really weird happened")
						}
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("returns a miss for a nonexistent field", func() {
				getResp, err := sharedContext.Client.DictionaryGetField(sharedContext.Ctx, &DictionaryGetFieldRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Field:          String("idontexist"),
				})
				Expect(err).To(BeNil())
				Expect(getResp).To(BeAssignableToTypeOf(&DictionaryGetFieldMiss{}))
			})

			It("returns a miss for a nonexistent dictionary", func() {
				getResp, err := sharedContext.Client.DictionaryGetField(sharedContext.Ctx, &DictionaryGetFieldRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: uuid.NewString(),
					Field:          String("idontexist"),
				})
				Expect(err).To(BeNil())
				Expect(getResp).To(BeAssignableToTypeOf(&DictionaryGetFieldMiss{}))
			})

			It("returns an error for a nil field", func() {
				Expect(
					sharedContext.Client.DictionaryGetField(sharedContext.Ctx, &DictionaryGetFieldRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
						Field:          nil,
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

		})

		When("getting multiple fields", func() {

			DescribeTable("returns the correct string and byte values",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					getResp, err := client.DictionaryGetFields(sharedContext.Ctx, &DictionaryGetFieldsRequest{
						CacheName:      cacheName,
						DictionaryName: sharedContext.CollectionName,
						Fields:         []Value{String("myField1"), String("myField2")},
					})
					Expect(err).To(BeNil())
					Expect(getResp).To(BeAssignableToTypeOf(&DictionaryGetFieldsHit{}))

					expectedStrings := map[string]string{"myField1": "myValue1", "myField2": "myValue2"}
					expectedBytes := map[string][]byte{"myField1": []byte("myValue1"), "myField2": []byte("myValue2")}
					switch result := getResp.(type) {
					case *DictionaryGetFieldsHit:
						Expect(result.ValueMapStringString()).To(Equal(expectedStrings))
						Expect(result.ValueMap()).To(Equal(expectedStrings))
						Expect(result.ValueMapStringBytes()).To(Equal(expectedBytes))
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("returns a miss for nonexistent dictionary", func() {
				Expect(
					sharedContext.Client.DictionaryGetFields(sharedContext.Ctx, &DictionaryGetFieldsRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: uuid.NewString(),
						Fields:         []Value{String("myField1")},
					}),
				).To(BeAssignableToTypeOf(&DictionaryGetFieldsMiss{}))
			})

			It("returns misses for nonexistent fields", func() {
				getResp, err := sharedContext.Client.DictionaryGetFields(sharedContext.Ctx, &DictionaryGetFieldsRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Fields:         []Value{String("bogusField1"), String("bogusField2")},
				})
				Expect(err).To(BeNil())
				Expect(getResp).To(BeAssignableToTypeOf(&DictionaryGetFieldsHit{}))
				switch result := getResp.(type) {
				case *DictionaryGetFieldsHit:
					Expect(result.ValueMap()).To(BeEmpty())
					for _, value := range result.Responses() {
						Expect(value).To(BeAssignableToTypeOf(&DictionaryGetFieldMiss{}))
					}
				}
			})

			It("filters missing fields out of response value maps", func() {
				getResp, err := sharedContext.Client.DictionaryGetFields(sharedContext.Ctx, &DictionaryGetFieldsRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Fields:         []Value{String("bogusField1"), String("myField2")},
				})
				Expect(err).To(BeNil())
				Expect(getResp).To(BeAssignableToTypeOf(&DictionaryGetFieldsHit{}))
				switch result := getResp.(type) {
				case *DictionaryGetFieldsHit:
					Expect(result.ValueMap()).To(Equal(map[string]string{"myField2": "myValue2"}))
					Expect(len(result.Responses())).To(Equal(2))
				}
			})

			It("returns an error for a nil field", func() {
				Expect(
					sharedContext.Client.DictionaryGetFields(sharedContext.Ctx, &DictionaryGetFieldsRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
						Fields:         []Value{String("myField"), nil},
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

		})
	})

	Describe("dictionary fetch", func() {

		BeforeEach(func() {
			Expect(
				sharedContext.Client.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Elements: DictionaryElementsFromMapStringValue(
						map[string]Value{"myField1": String("myValue1"), "myField2": Bytes("myValue2")},
					),
				}),
			).To(BeAssignableToTypeOf(&DictionarySetFieldsSuccess{}))
			Expect(
				sharedContext.ClientWithDefaultCacheName.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
					DictionaryName: sharedContext.CollectionName,
					Elements: DictionaryElementsFromMapStringValue(
						map[string]Value{"myField1": String("myValue1"), "myField2": Bytes("myValue2")},
					),
				}),
			).To(BeAssignableToTypeOf(&DictionarySetFieldsSuccess{}))
		})

		DescribeTable("fetches on the happy path",
			func(clientType string) {
				client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
				expected := map[string]string{"myField1": "myValue1", "myField2": "myValue2"}
				fetchResp, err := client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
					CacheName:      cacheName,
					DictionaryName: sharedContext.CollectionName,
				})
				Expect(err).To(BeNil())
				Expect(fetchResp).To(BeAssignableToTypeOf(&DictionaryFetchHit{}))
				switch result := fetchResp.(type) {
				case *DictionaryFetchHit:
					Expect(result.ValueMap()).To(Equal(expected))
				}
			},
			Entry("with default client", DefaultClient),
			Entry("with client with default cache", WithDefaultCache),
		)

		It("returns a miss for nonexistent dictionary", func() {
			Expect(
				sharedContext.Client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: uuid.NewString(),
				}),
			).To(BeAssignableToTypeOf(&DictionaryFetchMiss{}))
		})

	})

	Describe("dictionary remove", func() {

		BeforeEach(func() {
			Expect(
				sharedContext.Client.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Elements: DictionaryElementsFromMapStringValue(
						map[string]Value{
							"myField1": String("myValue1"),
							"myField2": Bytes("myValue2"),
							"myField3": String("myValue3"),
						},
					),
				}),
			).To(BeAssignableToTypeOf(&DictionarySetFieldsSuccess{}))
			Expect(
				sharedContext.ClientWithDefaultCacheName.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
					DictionaryName: sharedContext.CollectionName,
					Elements: DictionaryElementsFromMapStringValue(
						map[string]Value{
							"myField1": String("myValue1"),
							"myField2": Bytes("myValue2"),
							"myField3": String("myValue3"),
						},
					),
				}),
			).To(BeAssignableToTypeOf(&DictionarySetFieldsSuccess{}))
		})

		When("removing a single field", func() {

			DescribeTable("properly removes a field",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					removeResp, err := client.DictionaryRemoveField(sharedContext.Ctx, &DictionaryRemoveFieldRequest{
						CacheName:      cacheName,
						DictionaryName: sharedContext.CollectionName,
						Field:          String("myField1"),
					})
					Expect(err).To(BeNil())
					Expect(removeResp).To(BeAssignableToTypeOf(&DictionaryRemoveFieldSuccess{}))

					fetchResp, err := client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
						CacheName:      cacheName,
						DictionaryName: sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					switch result := fetchResp.(type) {
					case *DictionaryFetchHit:
						Expect(result.ValueMap()).To(Equal(map[string]string{
							"myField2": "myValue2",
							"myField3": "myValue3",
						}))
					default:
						Fail("expected a hit from dictionary fetch but got a miss")
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("no-ops when attempting to remove a nonexistent field", func() {
				removeResp, err := sharedContext.Client.DictionaryRemoveField(sharedContext.Ctx, &DictionaryRemoveFieldRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Field:          String("bogusField1"),
				})
				Expect(err).To(BeNil())
				Expect(removeResp).To(BeAssignableToTypeOf(&DictionaryRemoveFieldSuccess{}))
			})

			It("no-ops when using a nonexistent dictionary", func() {
				removeResp, err := sharedContext.Client.DictionaryRemoveField(sharedContext.Ctx, &DictionaryRemoveFieldRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: uuid.NewString(),
					Field:          String("bogusField1"),
				})
				Expect(err).To(BeNil())
				Expect(removeResp).To(BeAssignableToTypeOf(&DictionaryRemoveFieldSuccess{}))
			})

			It("returns an error when trying to remove a nil field", func() {
				Expect(
					sharedContext.Client.DictionaryRemoveField(sharedContext.Ctx, &DictionaryRemoveFieldRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
						Field:          nil,
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

		})

		When("removing multiple fields", func() {
			DescribeTable("properly removes multiple fields",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					removeResp, err := client.DictionaryRemoveFields(sharedContext.Ctx, &DictionaryRemoveFieldsRequest{
						CacheName:      cacheName,
						DictionaryName: sharedContext.CollectionName,
						Fields:         []Value{String("myField1"), Bytes("myField2")},
					})
					Expect(err).To(BeNil())
					Expect(removeResp).To(BeAssignableToTypeOf(&DictionaryRemoveFieldsSuccess{}))

					fetchResp, err := client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
						CacheName:      cacheName,
						DictionaryName: sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					switch result := fetchResp.(type) {
					case *DictionaryFetchHit:
						Expect(result.ValueMap()).To(Equal(map[string]string{
							"myField3": "myValue3",
						}))
					default:
						Fail("expected a hit from dictionary fetch but got a miss")
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("no-ops when attempting to remove a nonexistent field", func() {
				removeResp, err := sharedContext.Client.DictionaryRemoveFields(sharedContext.Ctx, &DictionaryRemoveFieldsRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Fields:         []Value{String("bogusField1"), Bytes("bogusField2")},
				})
				Expect(err).To(BeNil())
				Expect(removeResp).To(BeAssignableToTypeOf(&DictionaryRemoveFieldsSuccess{}))
			})

			It("no-ops when using a nonexistent dictionary", func() {
				removeResp, err := sharedContext.Client.DictionaryRemoveFields(sharedContext.Ctx, &DictionaryRemoveFieldsRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: uuid.NewString(),
					Fields:         []Value{String("bogusField1"), Bytes("bogusField2")},
				})
				Expect(err).To(BeNil())
				Expect(removeResp).To(BeAssignableToTypeOf(&DictionaryRemoveFieldsSuccess{}))
			})

			It("returns an error when Fields is nil", func() {
				Expect(
					sharedContext.Client.DictionaryRemoveFields(sharedContext.Ctx, &DictionaryRemoveFieldsRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
						Fields:         nil,
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

			It("returns an error when one field is nil", func() {
				Expect(
					sharedContext.Client.DictionaryRemoveFields(sharedContext.Ctx, &DictionaryRemoveFieldsRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
						Fields:         []Value{String("myField"), nil, String("myField2")},
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

		})
	})

	Describe("client TTL", func() {

		When("client TTL is exceeded", func() {

			It("returns a miss for the collection", func() {
				Expect(
					sharedContext.Client.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
						Elements: DictionaryElementsFromMapStringValue(
							map[string]Value{"myField1": String("myValue1"), "myField2": String("myValue2")},
						),
					}),
				).Error().To(BeNil())

				Expect(
					sharedContext.Client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
					}),
				).To(BeAssignableToTypeOf(&DictionaryFetchHit{}))

				time.Sleep(sharedContext.DefaultTtl)

				Expect(
					sharedContext.Client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
					}),
				).To(BeAssignableToTypeOf(&DictionaryFetchMiss{}))
			})

		})

	})

	Describe("collection TTL", func() {

		BeforeEach(func() {
			Expect(
				sharedContext.Client.DictionarySetFields(sharedContext.Ctx, &DictionarySetFieldsRequest{
					CacheName:      sharedContext.CacheName,
					DictionaryName: sharedContext.CollectionName,
					Elements: DictionaryElementsFromMapStringValue(
						map[string]Value{"myField1": String("myValue1"), "myField2": String("myValue2")},
					),
				}),
			).Error().To(BeNil())
		})

		When("collection TTL is empty", func() {

			It("will have a default ttl and refreshTtl and fetch will hit after client default ttl", func() {
				time.Sleep(sharedContext.DefaultTtl / 2)
				Expect(
					sharedContext.Client.DictionarySetField(sharedContext.Ctx, &DictionarySetFieldRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
						Field:          String("foo"),
						Value:          String("bar"),
					}),
				).To(BeAssignableToTypeOf(&DictionarySetFieldSuccess{}))

				time.Sleep(sharedContext.DefaultTtl / 2)

				Expect(
					sharedContext.Client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
					}),
				).To(BeAssignableToTypeOf(&DictionaryFetchHit{}))
			})

		})

		When("collection TTL is configured", func() {

			It("is ignored if refresh ttl is false", func() {
				Expect(
					sharedContext.Client.DictionarySetField(sharedContext.Ctx, &DictionarySetFieldRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
						Field:          String("myField3"),
						Value:          String("myValue3"),
						Ttl: &CollectionTtl{
							Ttl:        sharedContext.DefaultTtl + time.Second*60,
							RefreshTtl: false,
						},
					}),
				).To(BeAssignableToTypeOf(&DictionarySetFieldSuccess{}))

				time.Sleep(sharedContext.DefaultTtl)

				Expect(
					sharedContext.Client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
					}),
				).To(BeAssignableToTypeOf(&DictionaryFetchMiss{}))
			})

			It("is respected if refresh TTL is true", func() {
				Expect(
					sharedContext.Client.DictionarySetField(sharedContext.Ctx, &DictionarySetFieldRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
						Field:          String("myField3"),
						Value:          String("myValue3"),
						Ttl: &CollectionTtl{
							Ttl:        sharedContext.DefaultTtl + time.Second*60,
							RefreshTtl: true,
						},
					}),
				).To(BeAssignableToTypeOf(&DictionarySetFieldSuccess{}))

				time.Sleep(sharedContext.DefaultTtl)

				Expect(
					sharedContext.Client.DictionaryFetch(sharedContext.Ctx, &DictionaryFetchRequest{
						CacheName:      sharedContext.CacheName,
						DictionaryName: sharedContext.CollectionName,
					}),
				).To(BeAssignableToTypeOf(&DictionaryFetchHit{}))
			})

		})

	})

})
