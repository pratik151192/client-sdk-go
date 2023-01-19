package incubating

import (
	"context"
	"log"
	"net"
	"time"

	pb "github.com/momentohq/client-sdk-go/internal/protos"

	"google.golang.org/grpc"
)

type TestPubSubServer struct {
	pb.UnimplementedPubsubServer
	// TODO make this more sophisticated to support multiple subscriptions right now just support one global channel to start
	basicMessageChannel chan string
}

func newMockPubSubServer() {
	lis, err := net.Listen("tcp", "localhost:3000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPubsubServer(s, &TestPubSubServer{
		basicMessageChannel: make(chan string),
	})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (t TestPubSubServer) Publish(ctx context.Context, req *pb.XPublishRequest) (*pb.XEmpty, error) {
	time.Sleep(30 * time.Millisecond)
	t.basicMessageChannel <- req.Value.String() // TODO think about bytes vs strings
	return &pb.XEmpty{}, nil
}
func (t TestPubSubServer) Subscribe(req *pb.XSubscriptionRequest, server pb.Pubsub_SubscribeServer) error {
	count := 0
	for msg := range t.basicMessageChannel {
		err := server.SendMsg(&pb.XTopicItem{
			TopicSequenceNumber: uint64(count),
			Value: &pb.XTopicValue{
				Kind: &pb.XTopicValue_Text{
					Text: msg,
				},
			},
		})
		if err != nil {
			return err
		}
		count += 1
	}
	return nil
}
