use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const NUM_FILES: u32 = 100;
const FILE_SIZE: usize = 200 * 1024 * 1024;
const WRITE_CHUNK: usize = 1024 * 1024;

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

    futures::stream::iter(filenames)
        .map(|filename| async move {
            let mut buf = vec![0_u8; 4096];

            let start = std::time::Instant::now();
            let mut file = tokio::fs::File::open(filename).await.unwrap();
            let open_total_secs = start.elapsed().as_secs_f32();

            let start = std::time::Instant::now();
            file.read_exact(&mut buf).await.unwrap();
            let first_read_total_secs = start.elapsed().as_secs_f32();

            let start = std::time::Instant::now();
            file.seek(std::io::SeekFrom::Start(10 * 4096))
                .await
                .unwrap();
            file.read_exact(&mut buf).await.unwrap();
            let near_read_total_secs = start.elapsed().as_secs_f32();

            let start = std::time::Instant::now();
            file.seek(std::io::SeekFrom::Start(100 * 1024 * 1024))
                .await
                .unwrap();
            file.read_exact(&mut buf).await.unwrap();
            let far_read_total_secs = start.elapsed().as_secs_f32();

            println!(
                "{},{},{},{}",
                open_total_secs, first_read_total_secs, near_read_total_secs, far_read_total_secs
            );
        })
        .buffer_unordered(NUM_FILES as usize)
        .collect::<Vec<_>>()
        .await;
}
