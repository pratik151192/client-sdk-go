package momento_test

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
	. "github.com/momentohq/client-sdk-go/momento"
	. "github.com/momentohq/client-sdk-go/momento/test_helpers"
	. "github.com/momentohq/client-sdk-go/responses"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func getValueAndExpectedValueLists(numItems int) ([]Value, []string) {
	var values []Value
	var expected []string
	for i := 0; i < numItems; i++ {
		strVal := fmt.Sprintf("#%d", i)
		var value Value
		if i%2 == 0 {
			value = String(strVal)
		} else {
			value = Bytes(strVal)
		}
		values = append(values, value)
		expected = append(expected, strVal)
	}
	return values, expected
}

func populateList(sharedContext SharedContext, numItems int) []string {
	values, expected := getValueAndExpectedValueLists(numItems)
	Expect(
		sharedContext.Client.ListConcatenateFront(sharedContext.Ctx, &ListConcatenateFrontRequest{
			CacheName: sharedContext.CacheName,
			ListName:  sharedContext.CollectionName,
			Values:    values,
		}),
	).To(BeAssignableToTypeOf(&ListConcatenateFrontSuccess{}))
	Expect(
		sharedContext.ClientWithDefaultCacheName.ListConcatenateFront(sharedContext.Ctx, &ListConcatenateFrontRequest{
			ListName: sharedContext.CollectionName,
			Values:   values,
		}),
	).To(BeAssignableToTypeOf(&ListConcatenateFrontSuccess{}))
	return expected
}

var _ = Describe("List methods", func() {
	var sharedContext SharedContext

	BeforeEach(func() {
		sharedContext = NewSharedContext()
		sharedContext.CreateDefaultCaches()
		DeferCleanup(func() {
			sharedContext.Close()
		})
	})

	DescribeTable("try using invalid cache and list names",
		func(clientType string, cacheName string, listName string, expectedErrorCode string) {
			client, _ := sharedContext.GetClientPrereqsForType(clientType)
			Expect(
				client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
					CacheName: cacheName,
					ListName:  listName,
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				client.ListLength(sharedContext.Ctx, &ListLengthRequest{
					CacheName: cacheName,
					ListName:  listName,
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				client.ListConcatenateBack(sharedContext.Ctx, &ListConcatenateBackRequest{
					CacheName: cacheName,
					ListName:  listName,
					Values:    []Value{String("hi")},
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				client.ListConcatenateFront(sharedContext.Ctx, &ListConcatenateFrontRequest{
					CacheName: cacheName,
					ListName:  listName,
					Values:    []Value{String("hi")},
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				client.ListPopBack(sharedContext.Ctx, &ListPopBackRequest{
					CacheName: cacheName,
					ListName:  listName,
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				client.ListPopFront(sharedContext.Ctx, &ListPopFrontRequest{
					CacheName: cacheName,
					ListName:  listName,
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				client.ListPushFront(sharedContext.Ctx, &ListPushFrontRequest{
					CacheName: cacheName,
					ListName:  listName,
					Value:     String("hi"),
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				client.ListPushBack(sharedContext.Ctx, &ListPushBackRequest{
					CacheName: cacheName,
					ListName:  listName,
					Value:     String("hi"),
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))

			Expect(
				client.ListRemoveValue(sharedContext.Ctx, &ListRemoveValueRequest{
					CacheName: cacheName,
					ListName:  listName,
					Value:     String("hi"),
				}),
			).Error().To(HaveMomentoErrorCode(expectedErrorCode))
		},
		Entry("nonexistent cache name", DefaultClient, uuid.NewString(), uuid.NewString(), NotFoundError),
		Entry("empty cache name", DefaultClient, "", sharedContext.CollectionName, InvalidArgumentError),
		Entry("empty list name", DefaultClient, sharedContext.CacheName, "", InvalidArgumentError),
		Entry("nonexistent cache name", WithDefaultCache, uuid.NewString(), uuid.NewString(), NotFoundError),
		Entry("empty cache name", WithDefaultCache, "", sharedContext.CollectionName, InvalidArgumentError),
		Entry("empty list name", WithDefaultCache, sharedContext.CacheName, "", InvalidArgumentError),
	)

	It("returns the correct list length", func() {
		numItems := 33
		populateList(sharedContext, numItems)
		lengthResp, err := sharedContext.Client.ListLength(sharedContext.Ctx, &ListLengthRequest{
			CacheName: sharedContext.CacheName,
			ListName:  sharedContext.CollectionName,
		})
		Expect(err).To(BeNil())
		switch result := lengthResp.(type) {
		case *ListLengthHit:
			Expect(result.Length()).To(Equal(uint32(numItems)))
		default:
			Fail("expected a hit for list length but got a miss")
		}
	})

	Describe("list push", func() {

		When("pushing to the front of the list", func() {

			DescribeTable("pushes strings and bytes on the happy path",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 10
					values, expected := getValueAndExpectedValueLists(numItems)
					sort.Sort(sort.Reverse(sort.StringSlice(expected)))
					for _, value := range values {
						Expect(
							client.ListPushFront(sharedContext.Ctx, &ListPushFrontRequest{
								CacheName: cacheName,
								ListName:  sharedContext.CollectionName,
								Value:     value,
							}),
						).To(BeAssignableToTypeOf(&ListPushFrontSuccess{}))
					}
					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(BeAssignableToTypeOf(&ListFetchHit{}))
					Expect(fetchResp).To(HaveListLength(numItems))
					switch result := fetchResp.(type) {
					case *ListFetchHit:
						Expect(result.ValueList()).To(Equal(expected))
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			DescribeTable("truncates the list properly",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 10
					truncateTo := 5
					populateList(sharedContext, numItems)
					Expect(
						client.ListPushFront(sharedContext.Ctx, &ListPushFrontRequest{
							CacheName:          cacheName,
							ListName:           sharedContext.CollectionName,
							Value:              String("andherlittledogtoo"),
							TruncateBackToSize: uint32(truncateTo),
						}),
					).Error().To(BeNil())
					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(HaveListLength(truncateTo))
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("returns invalid argument for a nil value", func() {
				Expect(
					sharedContext.Client.ListPushBack(sharedContext.Ctx, &ListPushBackRequest{
						CacheName: sharedContext.CacheName,
						ListName:  sharedContext.CollectionName,
						Value:     nil,
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

			It("accepts an empty value", func() {
				Expect(
					sharedContext.Client.ListPushBack(sharedContext.Ctx, &ListPushBackRequest{
						CacheName: sharedContext.CacheName,
						ListName:  sharedContext.CollectionName,
						Value:     String(""),
					}),
				).To(BeAssignableToTypeOf(&ListPushBackSuccess{}))

				fetchResp, err := sharedContext.Client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
					CacheName: sharedContext.CacheName,
					ListName:  sharedContext.CollectionName,
				})
				Expect(err).To(BeNil())
				Expect(fetchResp).To(HaveListLength(1))
			})
		})

		When("pushing to the back of the list", func() {

			DescribeTable("pushes strings and bytes on the happy path",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 10
					values, expected := getValueAndExpectedValueLists(numItems)
					for _, value := range values {
						Expect(
							client.ListPushBack(sharedContext.Ctx, &ListPushBackRequest{
								CacheName: cacheName,
								ListName:  sharedContext.CollectionName,
								Value:     value,
							}),
						).To(BeAssignableToTypeOf(&ListPushBackSuccess{}))
					}

					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(HaveListLength(numItems))
					switch result := fetchResp.(type) {
					case *ListFetchHit:
						Expect(result.ValueList()).To(Equal(expected))
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			DescribeTable("truncates the list properly",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 10
					truncateTo := 5
					populateList(sharedContext, numItems)
					Expect(
						client.ListPushBack(sharedContext.Ctx, &ListPushBackRequest{
							CacheName:           cacheName,
							ListName:            sharedContext.CollectionName,
							Value:               String("andherlittledogtoo"),
							TruncateFrontToSize: uint32(truncateTo),
						}),
					).Error().To(BeNil())
					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(HaveListLength(truncateTo))
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("returns invalid argument for a nil value", func() {
				Expect(
					sharedContext.Client.ListPushBack(sharedContext.Ctx, &ListPushBackRequest{
						CacheName: sharedContext.CacheName,
						ListName:  sharedContext.CollectionName,
						Value:     nil,
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

			It("accepts an empty value", func() {
				Expect(
					sharedContext.Client.ListPushBack(sharedContext.Ctx, &ListPushBackRequest{
						CacheName: sharedContext.CacheName,
						ListName:  sharedContext.CollectionName,
						Value:     String(""),
					}),
				).To(BeAssignableToTypeOf(&ListPushBackSuccess{}))

				fetchResp, err := sharedContext.Client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
					CacheName: sharedContext.CacheName,
					ListName:  sharedContext.CollectionName,
				})
				Expect(err).To(BeNil())
				Expect(fetchResp).To(HaveListLength(1))
			})
		})

	})

	Describe("list concatenate", func() {

		When("concatenating to the front of the list", func() {

			DescribeTable("pushes strings and bytes on the happy path",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 10
					expected := populateList(sharedContext, numItems)

					numConcatItems := 5
					concatValues, concatExpected := getValueAndExpectedValueLists(numConcatItems)
					concatResp, err := client.ListConcatenateFront(sharedContext.Ctx, &ListConcatenateFrontRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
						Values:    concatValues,
					})
					Expect(err).To(BeNil())
					Expect(concatResp).To(BeAssignableToTypeOf(&ListConcatenateFrontSuccess{}))

					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(BeAssignableToTypeOf(&ListFetchHit{}))
					Expect(fetchResp).To(HaveListLength(numItems + numConcatItems))
					expected = append(concatExpected, expected...)
					switch result := fetchResp.(type) {
					case *ListFetchHit:
						Expect(result.ValueList()).To(Equal(expected))
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			DescribeTable("truncates the list properly",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					populateList(sharedContext, 5)
					concatValues := []Value{String("100"), String("101"), String("102")}
					concatResp, err := client.ListConcatenateFront(sharedContext.Ctx, &ListConcatenateFrontRequest{
						CacheName:          cacheName,
						ListName:           sharedContext.CollectionName,
						Values:             concatValues,
						TruncateBackToSize: 3,
					})
					Expect(err).To(BeNil())
					Expect(concatResp).To(BeAssignableToTypeOf(&ListConcatenateFrontSuccess{}))

					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(BeAssignableToTypeOf(&ListFetchHit{}))
					Expect(fetchResp).To(HaveListLength(3))
					switch result := fetchResp.(type) {
					case *ListFetchHit:
						Expect(result.ValueList()).To(Equal([]string{"100", "101", "102"}))
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("returns an invalid argument for a nil value", func() {
				populateList(sharedContext, 5)
				concatValues := []Value{nil, String("aRealValue"), nil}
				Expect(
					sharedContext.Client.ListConcatenateFront(sharedContext.Ctx, &ListConcatenateFrontRequest{
						CacheName:          sharedContext.CacheName,
						ListName:           sharedContext.CollectionName,
						Values:             concatValues,
						TruncateBackToSize: 3,
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

			It("accepts an empty value", func() {
				Expect(
					sharedContext.Client.ListConcatenateFront(sharedContext.Ctx, &ListConcatenateFrontRequest{
						CacheName: sharedContext.CacheName,
						ListName:  sharedContext.CollectionName,
						Values:    []Value{String("")},
					}),
				).To(BeAssignableToTypeOf(&ListConcatenateFrontSuccess{}))

				fetchResp, err := sharedContext.Client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
					CacheName: sharedContext.CacheName,
					ListName:  sharedContext.CollectionName,
				})
				Expect(err).To(BeNil())
				Expect(fetchResp).To(HaveListLength(1))
			})
		})

		When("concatenating to the back of the list", func() {

			DescribeTable("pushes strings and bytes on the happy path",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 10
					expected := populateList(sharedContext, numItems)

					numConcatItems := 5
					concatValues, concatExpected := getValueAndExpectedValueLists(numConcatItems)
					concatResp, err := client.ListConcatenateBack(sharedContext.Ctx, &ListConcatenateBackRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
						Values:    concatValues,
					})
					Expect(err).To(BeNil())
					Expect(concatResp).To(BeAssignableToTypeOf(&ListConcatenateBackSuccess{}))

					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(BeAssignableToTypeOf(&ListFetchHit{}))
					Expect(fetchResp).To(HaveListLength(numItems + numConcatItems))
					expected = append(expected, concatExpected...)
					switch result := fetchResp.(type) {
					case *ListFetchHit:
						Expect(result.ValueList()).To(Equal(expected))
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			DescribeTable("truncates the list properly",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					populateList(sharedContext, 5)
					concatValues := []Value{String("100"), String("101"), String("102")}
					concatResp, err := client.ListConcatenateBack(sharedContext.Ctx, &ListConcatenateBackRequest{
						CacheName:           cacheName,
						ListName:            sharedContext.CollectionName,
						Values:              concatValues,
						TruncateFrontToSize: 3,
					})
					Expect(err).To(BeNil())
					Expect(concatResp).To(BeAssignableToTypeOf(&ListConcatenateBackSuccess{}))

					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(BeAssignableToTypeOf(&ListFetchHit{}))
					Expect(fetchResp).To(HaveListLength(3))
					switch result := fetchResp.(type) {
					case *ListFetchHit:
						Expect(result.ValueList()).To(Equal([]string{"100", "101", "102"}))
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("returns an invalid argument for a nil value", func() {
				populateList(sharedContext, 5)
				concatValues := []Value{nil, String("aRealValue"), nil}
				Expect(
					sharedContext.Client.ListConcatenateBack(sharedContext.Ctx, &ListConcatenateBackRequest{
						CacheName:           sharedContext.CacheName,
						ListName:            sharedContext.CollectionName,
						Values:              concatValues,
						TruncateFrontToSize: 3,
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

			It("accepts an empty value", func() {
				Expect(
					sharedContext.Client.ListConcatenateBack(sharedContext.Ctx, &ListConcatenateBackRequest{
						CacheName: sharedContext.CacheName,
						ListName:  sharedContext.CollectionName,
						Values:    []Value{String("")},
					}),
				).To(BeAssignableToTypeOf(&ListConcatenateBackSuccess{}))

				fetchResp, err := sharedContext.Client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
					CacheName: sharedContext.CacheName,
					ListName:  sharedContext.CollectionName,
				})
				Expect(err).To(BeNil())
				Expect(fetchResp).To(HaveListLength(1))
			})
		})
	})

	Describe("list pop", func() {

		When("popping from the front of the list", func() {

			DescribeTable("pops strings and bytes on the happy path",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 5
					expected := populateList(sharedContext, numItems)

					popResp, err := client.ListPopFront(sharedContext.Ctx, &ListPopFrontRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					switch result := popResp.(type) {
					case *ListPopFrontHit:
						Expect(result.ValueString()).To(Equal(string(expected[0])))
					default:
						Fail("expected a hit from list pop front but got a miss")
					}

					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(HaveListLength(numItems - 1))
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("returns a miss after popping the last item", func() {
				numItems := 3
				populateList(sharedContext, numItems)
				for i := 0; i < 3; i++ {
					Expect(
						sharedContext.Client.ListPopFront(sharedContext.Ctx, &ListPopFrontRequest{
							CacheName: sharedContext.CacheName,
							ListName:  sharedContext.CollectionName,
						}),
					).To(BeAssignableToTypeOf(&ListPopFrontHit{}))
				}
				popResp, err := sharedContext.Client.ListPopFront(sharedContext.Ctx, &ListPopFrontRequest{
					CacheName: sharedContext.CacheName,
					ListName:  sharedContext.CollectionName,
				})
				Expect(err).To(BeNil())
				Expect(popResp).To(BeAssignableToTypeOf(&ListPopFrontMiss{}))
			})

		})

		When("popping from the back of the list", func() {

			DescribeTable("pops strings and bytes on the happy path",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 5
					expected := populateList(sharedContext, numItems)

					popResp, err := client.ListPopBack(sharedContext.Ctx, &ListPopBackRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					switch result := popResp.(type) {
					case *ListPopBackHit:
						Expect(result.ValueString()).To(Equal(string(expected[numItems-1])))
					default:
						Fail("expected a hit from list pop front but got a miss")
					}

					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(HaveListLength(numItems - 1))
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("returns a miss after popping the last item", func() {
				numItems := 3
				populateList(sharedContext, numItems)
				for i := 0; i < 3; i++ {
					Expect(
						sharedContext.Client.ListPopBack(sharedContext.Ctx, &ListPopBackRequest{
							CacheName: sharedContext.CacheName,
							ListName:  sharedContext.CollectionName,
						}),
					).To(BeAssignableToTypeOf(&ListPopBackHit{}))
				}
				popResp, err := sharedContext.Client.ListPopBack(sharedContext.Ctx, &ListPopBackRequest{
					CacheName: sharedContext.CacheName,
					ListName:  sharedContext.CollectionName,
				})
				Expect(err).To(BeNil())
				Expect(popResp).To(BeAssignableToTypeOf(&ListPopBackMiss{}))
			})

		})

	})

	Describe("list remove value", func() {

		When("removing a value that appears once", func() {

			DescribeTable("removes the value",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 5
					expected := populateList(sharedContext, numItems)
					Expect(
						client.ListRemoveValue(sharedContext.Ctx, &ListRemoveValueRequest{
							CacheName: cacheName,
							ListName:  sharedContext.CollectionName,
							Value:     String(expected[0]),
						}),
					).Error().To(BeNil())

					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					switch result := fetchResp.(type) {
					case *ListFetchHit:
						Expect(result.ValueList()).To(Equal(expected[1:]))
					default:
						Fail("expected a hit for list fetch but got a miss")
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("returns an error for a nil value", func() {
				Expect(
					sharedContext.Client.ListRemoveValue(sharedContext.Ctx, &ListRemoveValueRequest{
						CacheName: sharedContext.CacheName,
						ListName:  sharedContext.CollectionName,
						Value:     nil,
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

			It("accepts an empty value", func() {
				Expect(
					sharedContext.Client.ListConcatenateFront(
						sharedContext.Ctx,
						&ListConcatenateFrontRequest{
							CacheName: sharedContext.CacheName,
							ListName:  sharedContext.CollectionName,
							Values:    []Value{String("one"), String(""), String("three")},
						},
					),
				).To(BeAssignableToTypeOf(&ListConcatenateFrontSuccess{}))

				Expect(
					sharedContext.Client.ListRemoveValue(sharedContext.Ctx, &ListRemoveValueRequest{
						CacheName: sharedContext.CacheName,
						ListName:  sharedContext.CollectionName,
						Value:     String(""),
					}),
				).To(BeAssignableToTypeOf(&ListRemoveValueSuccess{}))

				fetchResp, err := sharedContext.Client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
					CacheName: sharedContext.CacheName,
					ListName:  sharedContext.CollectionName,
				})
				Expect(err).To(BeNil())
				switch result := fetchResp.(type) {
				case *ListFetchHit:
					Expect(
						result.ValueListString(),
					).To(Equal([]string{"one", "three"}))
				}
			})
		})

		When("removing a value that appears multiple times", func() {

			DescribeTable("removes all occurrences of the value",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 5
					populateList(sharedContext, numItems)
					toAdd := []Value{String("#4"), String("#4"), String("#4"), String("#0")}
					Expect(
						client.ListConcatenateBack(sharedContext.Ctx, &ListConcatenateBackRequest{
							CacheName: cacheName,
							ListName:  sharedContext.CollectionName,
							Values:    toAdd,
						}),
					).To(BeAssignableToTypeOf(&ListConcatenateBackSuccess{}))

					Expect(
						client.ListRemoveValue(sharedContext.Ctx, &ListRemoveValueRequest{
							CacheName: cacheName,
							ListName:  sharedContext.CollectionName,
							Value:     String("#4"),
						}),
					).To(BeAssignableToTypeOf(&ListRemoveValueSuccess{}))

					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					switch result := fetchResp.(type) {
					case *ListFetchHit:
						Expect(result.ValueList()).To(Equal([]string{"#0", "#1", "#2", "#3", "#0"}))
					default:
						Fail("expected a hit from list fetch but got a miss")
					}
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)

			It("returns an error for a nil value", func() {
				Expect(
					sharedContext.Client.ListRemoveValue(sharedContext.Ctx, &ListRemoveValueRequest{
						CacheName: sharedContext.CacheName,
						ListName:  sharedContext.CollectionName,
						Value:     nil,
					}),
				).Error().To(HaveMomentoErrorCode(InvalidArgumentError))
			})

		})

		When("removing a value that isn't in the list", func() {

			DescribeTable("returns success",
				func(clientType string) {
					client, cacheName := sharedContext.GetClientPrereqsForType(clientType)
					numItems := 5
					populateList(sharedContext, numItems)
					Expect(
						client.ListRemoveValue(sharedContext.Ctx, &ListRemoveValueRequest{
							CacheName: cacheName,
							ListName:  sharedContext.CollectionName,
							Value:     String("iamnotinthelist"),
						}),
					).To(BeAssignableToTypeOf(&ListRemoveValueSuccess{}))

					fetchResp, err := client.ListFetch(sharedContext.Ctx, &ListFetchRequest{
						CacheName: cacheName,
						ListName:  sharedContext.CollectionName,
					})
					Expect(err).To(BeNil())
					Expect(fetchResp).To(HaveListLength(numItems))
				},
				Entry("with default client", DefaultClient),
				Entry("with client with default cache", WithDefaultCache),
			)
		})

		When("removing from a nonexistent list", func() {

			It("returns success", func() {
				Expect(
					sharedContext.Client.ListRemoveValue(sharedContext.Ctx, &ListRemoveValueRequest{
						CacheName: sharedContext.CacheName,
						ListName:  uuid.NewString(),
						Value:     String("iamnotinthelist"),
					}),
				).To(BeAssignableToTypeOf(&ListRemoveValueSuccess{}))

			})

		})

	})

})
