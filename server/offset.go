package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Kaiya/kafka-chan/kafkapb"
	"github.com/Kaiya/kafka-chan/utils"
	"github.com/hoveychen/kafka-go"
	"github.com/pkg/errors"
	"gitlab.momoso.com/cm/inventory/data-processing/dataprocpb"
	"gitlab.momoso.com/mms2/utils/kafkautil"
	"gitlab.momoso.com/mms2/utils/lg"
)

func (s *Server) QueryMsgByOffset(ctx context.Context, in *kafkapb.QueryMsgByOffsetRequest) (*kafkapb.QueryMsgByOffsetReply, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: s.brokers,
		Dialer: &kafka.Dialer{
			Resolver:  utils.NewConsulKafkaResolver(s.brokers[0]),
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		Topic:          in.GetKafkaTopic(),
		Partition:      int(in.GetPartition()),
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second * 10,
	})
	err := reader.SetOffset(in.GetOffset())
	if err != nil {
		return nil, errors.Wrap(err, "reader setoffset")
	}
	msg, err := reader.FetchMessage(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fetch msg")
	}
	retByteArray := msg.Value
	if kafkautil.IsGzipCompressed(msg.Value) {
		uncompressed, err := decompress(msg.Value)
		if err != nil {
			return nil, err
		}
		lg.Info("decompressed done")
		retByteArray = uncompressed
	}
	var msgPb dataprocpb.AssemblerResponse
	err = json.Unmarshal(retByteArray, &msgPb)
	if err != nil {
		lg.Errorf("unmarshal error:%s", err)
	}
	return &kafkapb.QueryMsgByOffsetReply{
		MsgJson: string(retByteArray),
	}, nil
}
