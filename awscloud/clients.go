package awscloud

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/firehose"
)

// DynamoDB constructs a new reference to the AWS Dynamodb API
func DynamoDB(region, endpoint string) (*dynamodb.DynamoDB, error) {
	cfg := &aws.Config{
		Region: aws.String(region),
	}
	if endpoint != "" {
		cfg.Endpoint = aws.String(endpoint)
	}

	s, err := session.NewSession(cfg)
	if err != nil {
		return nil, err
	}
	return dynamodb.New(s), nil
}

// Firehose constructs a new reference to the AWS Firehose API
func Firehose(region string) (*firehose.Firehose, error) {
	cfg := &aws.Config{
		Region: aws.String(region),
	}

	s, err := session.NewSession(cfg)
	if err != nil {
		return nil, err
	}
	return firehose.New(s), nil
}

func wip() {
	f, _ := Firehose("as")
	f.CreateDeliveryStreamWithContext(context.Background(), &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("name"),
		ExtendedS3DestinationConfiguration: &firehose.ExtendedS3DestinationConfiguration{
			BucketARN: aws.String("arn"),
			BufferingHints: &firehose.BufferingHints{
				IntervalInSeconds: aws.Int64(50),
				SizeInMBs:         aws.Int64(20),
			},
			CloudWatchLoggingOptions: &firehose.CloudWatchLoggingOptions{
				Enabled:       aws.Bool(true),
				LogGroupName:  aws.String("group-name"),
				LogStreamName: aws.String("log-stream-name"),
			},
			CompressionFormat: aws.String("UNCOMPRESSED"),
			EncryptionConfiguration: &firehose.EncryptionConfiguration{
				KMSEncryptionConfig: &firehose.KMSEncryptionConfig{
					AWSKMSKeyARN: aws.String("123"),
				},
				NoEncryptionConfig: aws.String("???"),
			},
			Prefix: aws.String("prefix"),
			ProcessingConfiguration: &firehose.ProcessingConfiguration{
				Enabled:    aws.Bool(true),
				Processors: []*firehose.Processor{},
			},
			RoleARN:               aws.String("role-arn"),
			S3BackupConfiguration: &firehose.S3DestinationConfiguration{},
		},
	})
}
