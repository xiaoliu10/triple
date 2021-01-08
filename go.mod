module github.com/dubbogo/triple

go 1.15

require (
	github.com/apache/dubbo-go v1.5.5
	github.com/apache/dubbo-go-hessian2 v1.8.1
	github.com/golang/protobuf v1.4.3
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	google.golang.org/grpc v1.34.0
)

replace (
	github.com/apache/dubbo-go v1.5.5 => ../dubbo-go
)