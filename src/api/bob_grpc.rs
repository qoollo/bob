// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]


// interface

pub trait BobApi {
    fn put(&self, o: ::grpc::RequestOptions, p: super::bob::PutRequest) -> ::grpc::SingleResponse<super::bob::OpStatus>;

    fn get(&self, o: ::grpc::RequestOptions, p: super::bob::GetRequest) -> ::grpc::SingleResponse<super::bob::BlobResult>;
}

// client

pub struct BobApiClient {
    grpc_client: ::std::sync::Arc<::grpc::Client>,
    method_Put: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::bob::PutRequest, super::bob::OpStatus>>,
    method_Get: ::std::sync::Arc<::grpc::rt::MethodDescriptor<super::bob::GetRequest, super::bob::BlobResult>>,
}

impl ::grpc::ClientStub for BobApiClient {
    fn with_client(grpc_client: ::std::sync::Arc<::grpc::Client>) -> Self {
        BobApiClient {
            grpc_client: grpc_client,
            method_Put: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/bob_storage.BobApi/Put".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
            method_Get: ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                name: "/bob_storage.BobApi/Get".to_string(),
                streaming: ::grpc::rt::GrpcStreaming::Unary,
                req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
            }),
        }
    }
}

impl BobApi for BobApiClient {
    fn put(&self, o: ::grpc::RequestOptions, p: super::bob::PutRequest) -> ::grpc::SingleResponse<super::bob::OpStatus> {
        self.grpc_client.call_unary(o, p, self.method_Put.clone())
    }

    fn get(&self, o: ::grpc::RequestOptions, p: super::bob::GetRequest) -> ::grpc::SingleResponse<super::bob::BlobResult> {
        self.grpc_client.call_unary(o, p, self.method_Get.clone())
    }
}

// server

pub struct BobApiServer;


impl BobApiServer {
    pub fn new_service_def<H : BobApi + 'static + Sync + Send + 'static>(handler: H) -> ::grpc::rt::ServerServiceDefinition {
        let handler_arc = ::std::sync::Arc::new(handler);
        ::grpc::rt::ServerServiceDefinition::new("/bob_storage.BobApi",
            vec![
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/bob_storage.BobApi/Put".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.put(o, p))
                    },
                ),
                ::grpc::rt::ServerMethod::new(
                    ::std::sync::Arc::new(::grpc::rt::MethodDescriptor {
                        name: "/bob_storage.BobApi/Get".to_string(),
                        streaming: ::grpc::rt::GrpcStreaming::Unary,
                        req_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                        resp_marshaller: Box::new(::grpc::protobuf::MarshallerProtobuf),
                    }),
                    {
                        let handler_copy = handler_arc.clone();
                        ::grpc::rt::MethodHandlerUnary::new(move |o, p| handler_copy.get(o, p))
                    },
                ),
            ],
        )
    }
}
