use std::thread;
use std::time::Duration;

use azmq::AsyncContext;
use azmq::Message;
use azmq::Multipart;
use azmq::Result;

macro_rules! test_runtimes {
    (

        $( #[$meta:meta] )*
    //  ^~~~attributes~~~~^
        $vis:vis async fn $name:ident ( $( $arg_name:ident : $arg_ty:ty ),* $(,)? )
    //                          ^~~~~~~~~~~~~~~~argument list~~~~~~~~~~~~~~~^
            $( -> $ret_ty:ty )?
    //      ^~~~return type~~~^
            { $($tt:tt)* }
    //      ^~~~~body~~~~^
    ) => {
        ::paste::paste! {
        #[::async_std::test]
        $( #[$meta] )*
        $vis async fn [<async_std_$name>] ( $( $arg_name : $arg_ty ),* ) $( -> $ret_ty )? {
            $($tt)*
        }

        #[::tokio::test]
        $( #[$meta] )*
        $vis async fn [<tokio_st_$name>] ( $( $arg_name : $arg_ty ),* ) $( -> $ret_ty )? {
            $($tt)*
        }

        #[::tokio::test(flavor = "multi_thread", worker_threads = 2)]
        $( #[$meta] )*
        $vis async fn [<tokio_mt_$name>] ( $( $arg_name : $arg_ty ),* ) $( -> $ret_ty )? {
            $($tt)*
        }
        }
    }
}

test_runtimes! {
    async fn test_poll() -> Result<()> {
        let context = AsyncContext::new()?;
        let mut sender = context.socket(zmq::SocketType::PAIR)?;
        sender.inner().bind("inproc://test")?;
        let mut receiver = context.socket(zmq::SocketType::PAIR)?;
        receiver.inner().connect("inproc://test")?;

        println!("Starting test");

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            sender.blocking_send(Message::from(&[0, 1][..])).unwrap();
            thread::sleep(Duration::from_millis(50));
            sender.blocking_send(Message::from(&[2, 3][..])).unwrap();
        });

        let recv = receiver.recv().await.unwrap();
        assert_eq!(recv[0][..], [0, 1]);
        let recv = receiver.recv().await.unwrap();
        assert_eq!(recv[0][..], [2, 3]);

        Ok(())
    }
}

test_runtimes! {
    async fn test_direct() -> Result<()> {
        let context = AsyncContext::new()?;
        let mut sender = context.socket(zmq::SocketType::PAIR)?;
        sender.inner().bind("inproc://test")?;
        let mut receiver = context.socket(zmq::SocketType::PAIR)?;
        receiver.inner().connect("inproc://test")?;

        sender.blocking_send(Message::from(&[0, 1][..])).unwrap();

        let recv = receiver.recv().await.unwrap();
        assert_eq!(recv[0][..], [0, 1]);

        Ok(())
    }
}

test_runtimes! {
    async fn test_multiple() -> Result<()> {
        let context = AsyncContext::new()?;
        let mut sender = context.socket(zmq::SocketType::DEALER)?;
        sender.inner().bind("inproc://test")?;
        let mut receiver1 = context.socket(zmq::SocketType::REP)?;
        receiver1.inner().connect("inproc://test")?;
        let mut receiver2 = context.socket(zmq::SocketType::REP)?;
        receiver2.inner().connect("inproc://test")?;
        let mut receiver3 = context.socket(zmq::SocketType::REP)?;
        receiver3.inner().connect("inproc://test")?;

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            sender.blocking_send(Message::from(&"message1")).unwrap();
            sender.blocking_send(Message::from(&"message2")).unwrap();
            sender.blocking_send(Message::from(&"message3")).unwrap();
            thread::sleep(Duration::from_millis(50));
            sender.blocking_send(Multipart::from([Message::empty(), Message::from(&"message1")])).unwrap();
            sender.blocking_send(Multipart::from([Message::empty(), Message::from(&"message2")])).unwrap();
            sender.blocking_send(Multipart::from([Message::empty(), Message::from(&"message3")])).unwrap();
        });

        let (first, second, third) = tokio::join!(
            receiver3.recv(),
            receiver1.recv(),
            receiver2.recv(),
        );
        assert_eq!(&first?[0][..], "message3".as_bytes());
        assert_eq!(&second?[0][..], "message1".as_bytes());
        assert_eq!(&third?[0][..], "message2".as_bytes());

        Ok(())
    }
}
