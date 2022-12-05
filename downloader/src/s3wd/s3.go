package s3wd

import (
	"bytes"
	"context"
	"goquery-client/src/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func UploadFile(s3w *s3.Client, bucket, key string, body []byte) {

	r := bytes.NewReader(body)
	uploader := manager.NewUploader(s3w)
	_, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		utils.FailOnError("s3", err)
	}
}
