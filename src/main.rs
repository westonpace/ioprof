use std::fs::File;
use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use rand::thread_rng;
use rand::{seq::SliceRandom, Rng};
use std::os::unix::fs::FileExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const NUM_FILES: u32 = 128;
const FILE_SIZE: usize = 200 * 1024 * 1024;
const WRITE_CHUNK: usize = 1024 * 1024;

/// Reads a range of data.
async fn get_range(file: Arc<File>, range: Range<usize>) -> Bytes {
    tokio::task::spawn_blocking(move || {
        let mut buf = BytesMut::with_capacity(range.len());
        // Safety: `buf` is set with appropriate capacity above. It is
        // written to below and we check all data is initialized at that point.
        unsafe { buf.set_len(range.len()) };
        file.read_exact_at(buf.as_mut(), range.start as u64)
            .unwrap();

        buf.freeze()
    })
    .await
    .unwrap()
}

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        println!("Expected one argument (data directory)");
        return;
    }
    let datadir = std::path::Path::new(&args[1]);

    let datadir_meta = std::fs::metadata(datadir).unwrap();
    if !datadir_meta.is_dir() {
        println!(
            "Expected the data directory to be a directory but got {}",
            datadir.display()
        );
        return;
    }

    let filenames = (0..NUM_FILES)
        .map(|idx| datadir.join(format!("{}.data", idx)))
        .collect::<Vec<_>>();

    let buf = vec![0_u8; WRITE_CHUNK];
    for filename in &filenames {
        if !filename.exists() {
            println!("Creating test data file: {}", filename.display());
            let mut file = tokio::fs::File::create(filename).await.unwrap();
            let mut bytes_written = 0_usize;
            while bytes_written < FILE_SIZE {
                file.write_all(&buf).await.unwrap();
                bytes_written += WRITE_CHUNK;
            }
            file.flush().await.unwrap();
        }
    }

    let full_start = std::time::Instant::now();
    futures::stream::iter(filenames)
        .map(|filename| async move {
            let thread_start = std::time::Instant::now();

            let start = std::time::Instant::now();
            let file = Arc::new(
                tokio::fs::File::open(filename)
                    .await
                    .unwrap()
                    .into_std()
                    .await,
            );
            let open_total_secs = start.elapsed().as_secs_f32();

            let start = std::time::Instant::now();
            get_range(file.clone(), 0..4096).await;
            let first_read_total_secs = start.elapsed().as_secs_f32();

            let start = std::time::Instant::now();
            get_range(file.clone(), (10 * 4096)..(11 * 4096)).await;
            let near_read_total_secs = start.elapsed().as_secs_f32();

            // Pick read locations that are 512KiB apart and shuffle them and then read the first two
            let mut locations = (100..256).map(|i| i * 512 * 1024).collect::<Vec<usize>>();
            locations.shuffle(&mut thread_rng());

            let mut far_read_total_secs = 0.0;
            for location in &locations[0..2] {
                let start = std::time::Instant::now();
                get_range(file.clone(), (*location)..(*location + 4096)).await;
                far_read_total_secs += start.elapsed().as_secs_f32();
            }

            let far_read_avg_secs = far_read_total_secs / locations.len() as f32;

            println!(
                "{},{},{},{},{}",
                open_total_secs,
                first_read_total_secs,
                near_read_total_secs,
                far_read_avg_secs,
                thread_start.elapsed().as_secs_f32()
            );
        })
        .buffer_unordered(NUM_FILES as usize)
        .collect::<Vec<_>>()
        .await;
    println!(
        "Total elapsed seconds: {}",
        full_start.elapsed().as_secs_f32()
    );
}
