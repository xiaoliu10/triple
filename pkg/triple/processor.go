/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or codecied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package triple

import (
	"bytes"
	"errors"
)
import (
	dubboCommon "github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/logger"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)
import (
	"github.com/dubbogo/triple/internal/codec"
	"github.com/dubbogo/triple/internal/codes"
	"github.com/dubbogo/triple/internal/status"
	"github.com/dubbogo/triple/pkg/common"
)

// processor is the interface, with func runRPC
type processor interface {
	runRPC()
	close()
}

// baseProcessor is the basic impl of porcessor, which contains four base fields
type baseProcessor struct {
	stream     *serverStream
	pkgHandler common.PackageHandler
	serializer common.Dubbo3Serializer
	closeChain chan struct{}
}

func (s *baseProcessor) handleRPCStatusErr(err error) {
	appStatus, ok := status.FromError(err)
	if !ok {
		err = status.Errorf(codes.Unknown, err.Error())
		appStatus, _ = status.FromError(err)
	}
	s.stream.WriteStatus(appStatus)
}

func (bp *baseProcessor) close() {
	bp.closeChain <- struct{}{}
	close(bp.closeChain)
}

// unaryProcessor used to process unary invocation
type unaryProcessor struct {
	baseProcessor
	methodDesc grpc.MethodDesc
}

// newUnaryProcessor can create unary processor
func newUnaryProcessor(s *serverStream, pkgHandler common.PackageHandler, desc grpc.MethodDesc) (processor, error) {
	serilizer, err := common.GetDubbo3Serializer(codec.DefaultDubbo3SerializerName)
	if err != nil {
		logger.Error("newProcessor with serlizationg ", codec.DefaultDubbo3SerializerName, " error")
		return nil, err
	}

	return &unaryProcessor{
		baseProcessor: baseProcessor{
			serializer: serilizer,
			stream:     s,
			pkgHandler: pkgHandler,
			closeChain: make(chan struct{}, 1),
		},
		methodDesc: desc,
	}, nil
}

// processUnaryRPC can process unary rpc
func (p *unaryProcessor) processUnaryRPC(buf bytes.Buffer, service dubboCommon.RPCService, header common.ProtocolHeader) ([]byte, error) {
	readBuf := buf.Bytes()

	pkgData := p.pkgHandler.Frame2PkgData(readBuf)

	descFunc := func(v interface{}) error {
		if err := p.serializer.Unmarshal(pkgData, v.(proto.Message)); err != nil {
			return status.Errorf(codes.Internal, "Unary rpc request unmarshal error: %s", err)
		}
		return nil
	}

	reply, err := p.methodDesc.Handler(service, header.FieldToCtx(), descFunc, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Unary rpc handle error: %s", err)
	}

	replyData, err := p.serializer.Marshal(reply.(proto.Message))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Unary rpc reoly marshal error: %s", err)
	}

	rspFrameData := p.pkgHandler.Pkg2FrameData(replyData)
	return rspFrameData, nil
}

// runRPC is called by lower layer's stream
func (s *unaryProcessor) runRPC() {
	recvChan := s.stream.getRecv()
	go func() {
		select {
		case <-s.closeChain:
			logger.Info("unaryProcessor closed!")
			s.handleRPCStatusErr(status.Errorf(codes.Canceled, "processor has been canceled!"))
			return
		case recvMsg := <-recvChan:
			go func() {
				defer func() {
					if e := recover(); e != nil {
						s.handleRPCStatusErr(errors.New(e.(string)))
					}
				}()
				if recvMsg.err != nil {
					logger.Error("error ,s.processUnaryRPC err = ", recvMsg.err)
					s.handleRPCStatusErr(status.Errorf(codes.Internal, "error ,s.processUnaryRPC err = %s", recvMsg.err))
					return
				}
				rspData, err := s.processUnaryRPC(*recvMsg.buffer, s.stream.getService(), s.stream.getHeader())

				if err != nil {
					s.handleRPCStatusErr(err)
					return
				}
				// TODO: status sendResponse should has err, then writeStatus(err) use one function and defer
				// it's enough that unary processor just send data msg to stream layer
				// rpc status logic just let stream layer to handle
				s.stream.putSend(rspData, DataMsgType)
			}()
		}
	}()
}

// streamingProcessor used to process streaming invocation
type streamingProcessor struct {
	baseProcessor
	streamDesc grpc.StreamDesc
}

// newStreamingProcessor can create new streaming processor
func newStreamingProcessor(s *serverStream, pkgHandler common.PackageHandler, desc grpc.StreamDesc) (processor, error) {
	serilizer, err := common.GetDubbo3Serializer(codec.DefaultDubbo3SerializerName)
	if err != nil {
		logger.Error("newProcessor with serlizationg ", codec.DefaultDubbo3SerializerName, " error")
		return nil, err
	}

	return &streamingProcessor{
		baseProcessor: baseProcessor{
			serializer: serilizer,
			stream:     s,
			pkgHandler: pkgHandler,
			closeChain: make(chan struct{}),
		},
		streamDesc: desc,
	}, nil
}

// runRPC called by stream
func (sp *streamingProcessor) runRPC() {
	serverUserstream := newServerUserStream(sp.stream, sp.serializer, sp.pkgHandler)
	go func() {
		sp.streamDesc.Handler(sp.stream.getService(), serverUserstream)
		// for stream rpc, processor should send CloseMsg to lower stream layer to call close
		// but unary rpc not, unary rpc processor only send data to stream layer
		sp.stream.putSend(nil, ServerStreamCloseMsgType)
	}()
}
