package server

import (
	"context"
	"testing"
	"time"

	"github.com/Kaiya/kafka-chan/utils"
	"github.com/hoveychen/kafka-go"
	"gitlab.momoso.com/mms2/utils/kafkautil"
	"gitlab.momoso.com/mms2/utils/lg"
)

func TestDecode(t *testing.T) {
	// topic := "publisher-v2-downstream"
	topic := "assembler-v2-downstream"
	partition := 0
	offset := 12676
	brokers := []string{"kafka"}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Dialer: &kafka.Dialer{
			Resolver:  utils.NewConsulKafkaResolver(brokers[0]),
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		Topic:          topic,
		Partition:      partition,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second * 10,
	})
	err := reader.SetOffset(int64(offset))
	if err != nil {
		t.Errorf("reader setoffset error:%s", err)
	}
	msg, err := reader.FetchMessage(context.Background())
	if err != nil {
		// return nil, errors.Wrap(err, "fetch msg")
		t.Errorf("fetch msg error:%s ", err)
	}
	retByteArray := msg.Value
	if kafkautil.IsGzipCompressed(msg.Value) {
		t.Log("decoding")
		uncompressed, err := decompress(msg.Value)
		if err != nil {
			// return nil, err
			t.Errorf("decompress msg error: %s", err)
		}
		lg.Info("decompressed done")
		retByteArray = uncompressed
	}
	_ = retByteArray
}
