use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const NUM_FILES: u32 = 10;
const FILE_SIZE: usize = 1024 * 1024 * 1024;
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

    let mut open_total_secs = 0.0;
    let mut first_read_total_secs = 0.0;
    let mut near_read_total_secs = 0.0;
    let mut far_read_total_secs = 0.0;

    for filename in &filenames {
        let mut buf = vec![0_u8; 4096];

        let start = std::time::Instant::now();
        let mut file = tokio::fs::File::open(filename).await.unwrap();
        open_total_secs += start.elapsed().as_secs_f32();

        let start = std::time::Instant::now();
        file.read_exact(&mut buf).await.unwrap();
        first_read_total_secs += start.elapsed().as_secs_f32();

        let start = std::time::Instant::now();
        file.seek(std::io::SeekFrom::Start(10 * 4096))
            .await
            .unwrap();
        file.read_exact(&mut buf).await.unwrap();
        near_read_total_secs += start.elapsed().as_secs_f32();

        let start = std::time::Instant::now();
        file.seek(std::io::SeekFrom::Start(100 * 1024 * 1024))
            .await
            .unwrap();
        file.read_exact(&mut buf).await.unwrap();
        far_read_total_secs += start.elapsed().as_secs_f32();
    }

    let open_avg_secs = open_total_secs / filenames.len() as f32;
    println!("Open file average time : {}", open_avg_secs);

    let first_read_avg_secs = first_read_total_secs / filenames.len() as f32;
    println!("First read average time: {}", first_read_avg_secs);

    let near_read_avg_secs = near_read_total_secs / filenames.len() as f32;
    println!("Near read average time : {}", near_read_avg_secs);

    let far_read_avg_secs = far_read_total_secs / filenames.len() as f32;
    println!("Far read average time : {}", far_read_avg_secs);
}
