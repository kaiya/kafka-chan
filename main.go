package main

import (
	"github.com/Kaiya/kafka-chan/kafkapb"
	"github.com/Kaiya/kafka-chan/server"
	"github.com/Kaiya/kafka-chan/web"
	"gitlab.momoso.com/cm/kit/third_party/lg"
	"gitlab.momoso.com/mms2/utils/flags"
	"gitlab.momoso.com/mms2/utils/service"
	"google.golang.org/grpc"
)

var (
	port       = flags.Int("port", 8756, "server listen port")
	consumerId = flags.String("consumerId", "kafka-channel-consumer", "consumer group ID for consuming kafka msg")
	brokers    = flags.Slice("brokers", []string{"kafka"}, "kafka brokers")
)

func main() {
	flags.Parse()
	if len(brokers()) < 1 {
		panic("brokers is empty")
	}

	server := server.NewServer(consumerId(), brokers())
	webSrv := web.NewWebServer(*server)
	ms := service.NewMicroService(
		service.WithConsulName(flags.GetServiceName()),
		service.WithGRPC(func(srv *grpc.Server) {
			kafkapb.RegisterKafkaChanServer(srv, server)
		}),
		service.WithGRPCUI(),
		service.WithPrometheus(),
		service.WithPprof(),
		service.WithHttpHandler("/", webSrv),
		// service.WithTLS(),
	)
	lg.PanicError(ms.ListenAndServe(port()))
}
