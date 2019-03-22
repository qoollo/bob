
use tokio::prelude::{Future};

//use futures::future::;
use futures::{Poll};
use futures::future::Either;
use crate::core::backend::{Backend};
use crate::core::sprinkler::{Sprinkler};
use crate::core::data::{BobKey, BobData, BobOptions, };


pub struct Grinder {
    pub backend: Backend,
    pub sprinkler: Sprinkler
}


pub struct GrinderOk {}
pub struct GrinderError {}

pub struct GrinderPutResponse<T>(T);

impl<T> Future for GrinderPutResponse<T> 
    where T: Future, {
    type Item= T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }

}

type GrinderResult = std::result::Result<GrinderOk, GrinderError>;
impl Grinder {
    pub fn put(&self, key: BobKey, data: BobData, opts: BobOptions) -> 
        impl Future<Item = GrinderOk, Error = GrinderError> + 'static + Send {
        Box::new(if opts.contains(BobOptions::FORCE_NODE) {
            println!("PUT[{:?}] FORCE_NODE will handle it by myself", key);
            Either::A(self.backend.put(key, data).then(|_r| {
                            let k: GrinderResult = 
                            Ok(GrinderOk{});
                            k
                        }))
        } else {
            println!("PUT[{:?}] will forward it to cluster", key);
            Either::B(self.sprinkler.put_clustered(key, data).then(|_r| {
                            let k: GrinderResult = 
                            Ok(GrinderOk{});
                            k
                        }))
        })
        
    }
    // pub fn put(&self, key: BobKey, data: BobData, opts: BobOptions) -> GrinderPutResponse<Box<dyn Future<Item = GrinderOk, Error = GrinderError> + '_>> {
    //     if opts.contains(BobOptions::FORCE_NODE) {
    //         GrinderPutResponse(Box::new(self.backend.put(key, data).then(|r| {
    //                         let k: GrinderResult = 
    //                         Ok(GrinderOk{});
    //                         k
    //                     })))
    //     } else {
    //         let t = self.sprinkler.put_clustered(key, data).then(|r| {
    //                         let k: GrinderResult = 
    //                         Ok(GrinderOk{});
    //                         k
    //                     }); 
    //         GrinderPutResponse(Box::new(t))
    //     }
        
    // }
}