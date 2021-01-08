module github.com/dubbogo/triple

go 1.15

//replace github.com/apache/dubbo-go v1.5.5 => ../dubbo-go

require (
	github.com/apache/dubbo-go v1.5.5
	github.com/golang/protobuf v1.4.3
	github.com/pkg/errors v0.9.1
	golang.org/x/net v0.0.0-20201224014010-6772e930b67b
	google.golang.org/genproto v0.0.0-20210106152847-07624b53cd92
	google.golang.org/grpc v1.34.0
)
