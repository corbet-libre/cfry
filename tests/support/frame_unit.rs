use super::*;

#[test]
fn round_trip_in_arbitrary_chunks() {
    let mut sender = FrameCodec::new(128).unwrap();
    let mut receiver = FrameCodec::new(128).unwrap();
    let mut wire = sender.encode(b"hello").unwrap();
    wire.extend(sender.encode(b"").unwrap());
    wire.extend(sender.encode(&[7; 128]).unwrap());
    assert_eq!(&wire[..4], &5u32.to_be_bytes());
    let mut frames = Vec::new();
    for chunk in wire.chunks(3) {
        frames.extend(receiver.push(chunk).unwrap());
    }
    assert_eq!(frames, vec![b"hello".to_vec(), vec![], vec![7; 128]]);
    assert!(receiver.finish().is_ok());
}

#[test]
fn oversized_frames_close_the_codec() {
    let mut codec = FrameCodec::new(16).unwrap();
    assert_eq!(codec.encode(&[0; 17]).unwrap_err().kind(), ErrorKind::Limit);
    assert_eq!(codec.encode(b"x").unwrap_err().kind(), ErrorKind::Closed);

    let mut receiver = FrameCodec::new(16).unwrap();
    let header = 17u32.to_be_bytes();
    assert_eq!(
        receiver.push(&header).unwrap_err().kind(),
        ErrorKind::Protocol
    );
    assert_eq!(receiver.push(b"x").unwrap_err().kind(), ErrorKind::Closed);
}

#[test]
fn truncated_stream_is_an_error() {
    let mut codec = FrameCodec::new(16).unwrap();
    assert!(codec.push(&[0, 0, 0, 4, 1]).unwrap().is_empty());
    assert_eq!(codec.finish().unwrap_err().kind(), ErrorKind::Protocol);
}

#[test]
fn bounds() {
    assert!(FrameCodec::new(0).is_err());
    assert!(FrameCodec::new(MAX_FRAME_BYTES + 1).is_err());
    assert!(FrameCodec::new(MAX_FRAME_BYTES).is_ok());
    let mut codec = FrameCodec::new(8).unwrap();
    assert_eq!(codec.push(&[0; 13]).unwrap_err().kind(), ErrorKind::Limit);
}
