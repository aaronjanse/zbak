use chrono::{Datelike, Duration, DurationRound, TimeZone, Utc};
use clap::Clap;
use std::{
    ops::Sub,
    process::{Command, Stdio},
};

#[derive(Clap)]
#[clap(
    setting = clap::AppSettings::ColoredHelp,
    setting = clap::AppSettings::DeriveDisplayOrder,
    setting = clap::AppSettings::VersionlessSubcommands,
)]

pub struct App {
    #[clap(subcommand)]
    subcmd: Subcommand,
}

#[derive(Clap)]
enum Subcommand {
    Snap(SnapCommand),
    Send(SendCommand),
}

/// Replicates snapshots
#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct SendCommand {
    #[clap(long = "name")]
    name: String,
    #[clap(long = "from")]
    from: String,
    #[clap(long = "to")]
    to: String,
    #[clap(long = "keep")]
    keep: String,
}
/// Creates and prunes snapshots
#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct SnapCommand {
    location: String,
    #[clap(long = "keep")]
    keep: String,
}

#[allow(clippy::upper_case_acronyms)]
enum Transport {
    Local,
    SSH(String),
}

struct Remote {
    dataset: String,
    transport: Transport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Snapshot {
    path: String,
    time: chrono::DateTime<Utc>,
}

fn is_normal_snapshot(path: &str) -> bool {
    let re = regex::Regex::new(r"^[a-z/]+@\d{4}-\d{2}-\d{2}T\d{4}$").unwrap();
    re.is_match(path)
}

impl Remote {
    fn cmd(&self, args: &[&str]) -> Command {
        let mut cmd = match &self.transport {
            Transport::Local => std::process::Command::new("zfs"),
            Transport::SSH(host) => {
                let mut tmp = std::process::Command::new("ssh");
                tmp.args(&["-o", "ConnectTimeout=20", host]);
                tmp.arg("zfs");
                tmp
            }
        };
        cmd.args(args);
        cmd
    }

    fn exec(&self, args: &[&str]) -> Result<String, String> {
        let out = self.cmd(args).output().unwrap();
        if out.status.success() {
            Ok(String::from_utf8(out.stdout).unwrap())
        } else {
            Err(String::from_utf8(out.stderr).unwrap())
        }
    }

    fn internal_list_snapshots(&self) -> Vec<Snapshot> {
        let out = match self.exec(&[
            "list",
            "-t",
            "snapshot",
            "-o",
            "name,creation",
            "-Hp",
            &self.dataset,
        ]) {
            Ok(x) => x,
            Err(e) => {
                if e.contains("does not exist") {
                    "".to_string()
                } else {
                    panic!("cmd err: {}", e);
                }
            }
        };

        out.lines()
            .map(|line| {
                let parts = line.split('\t').collect::<Vec<_>>();
                Snapshot {
                    path: parts[0].to_string(),
                    time: chrono::Utc.timestamp(parts[1].parse::<i64>().unwrap(), 0),
                }
            })
            .collect()
    }

    fn list_snapshots(&self) -> Vec<Snapshot> {
        self.internal_list_snapshots()
            .into_iter()
            .filter(|snap| is_normal_snapshot(&snap.path))
            .collect()
    }

    fn list_bookmarks(&self, name: &str) -> Vec<Snapshot> {
        let out = match self.exec(&[
            "list",
            "-t",
            "bookmark",
            "-o",
            "name,creation",
            "-Hp",
            &self.dataset,
        ]) {
            Ok(x) => x,
            Err(e) => {
                if e.contains("does not exist") {
                    "".to_string()
                } else {
                    panic!("cmd err: {}", e);
                }
            }
        };

        let re = regex::Regex::new(r"^[a-z/]+#\d{4}-\d{2}-\d{2}T\d{4}-sync-").unwrap();

        out.lines()
            .map(|line| {
                let parts = line.split('\t').collect::<Vec<_>>();
                Snapshot {
                    path: parts[0].to_string(),
                    time: chrono::Utc.timestamp(parts[1].parse::<i64>().unwrap(), 0),
                }
            })
            .filter(|snap| {
                re.is_match(&snap.path) && snap.path.ends_with(&("-sync-".to_string() + name))
            })
            .collect()
    }

    fn snapshot(&self, path: &str) {
        self.exec(&["snapshot", path]).unwrap();
    }

    fn bookmark(&self, base: &str, mark: &str) {
        self.exec(&["bookmark", base, mark]).unwrap();
    }

    fn destroy_snapshot(&self, path: &str) {
        if !path.contains('@') {
            panic!("invalid path for snapshot");
        }
        self.exec(&["destroy", path]).unwrap();
    }

    fn destroy_bookmark(&self, path: &str) {
        if !path.contains('#') {
            panic!("invalid path for bookmark");
        }
        self.exec(&["destroy", path]).unwrap();
    }
}

#[derive(Debug)]
struct Spec {
    monthly: u64,
    weekly: u64,
    daily: u64,
    hourly: u64,
    frequently: u64,
}

struct PruningPlan {
    keep: Vec<Snapshot>,
    remove: Vec<Snapshot>,
}

fn find_prunable(
    now: &chrono::DateTime<Utc>,
    spec: &Spec,
    mut snapshots: Vec<Snapshot>,
) -> PruningPlan {
    snapshots.sort_by(|a, b| a.time.cmp(&b.time));

    let mut wanted = Vec::new();

    let mut cursor_month = now
        .with_day(1)
        .unwrap()
        .duration_round(Duration::days(1))
        .unwrap();
    for _ in 0..spec.monthly {
        wanted.push(cursor_month);
        cursor_month = if cursor_month.month() == 1 {
            cursor_month
                .with_year(cursor_month.year() - 1)
                .unwrap()
                .with_month(12)
                .unwrap()
        } else {
            cursor_month.with_month(cursor_month.month() - 1).unwrap()
        };
    }

    let mut cursor_week = now
        .sub(Duration::days(now.weekday().num_days_from_monday().into()))
        .duration_round(Duration::days(1))
        .unwrap();
    for _ in 0..spec.weekly {
        wanted.push(cursor_week);
        cursor_week = cursor_week.sub(Duration::days(7));
    }

    let mut cursor_day = now.duration_round(Duration::days(1)).unwrap();
    for _ in 0..spec.daily {
        wanted.push(cursor_day);
        cursor_day = cursor_day.sub(Duration::days(1));
    }

    let mut cursor_hour = now.duration_round(Duration::hours(1)).unwrap();
    for _ in 0..spec.hourly {
        wanted.push(cursor_hour);
        cursor_hour = cursor_hour.sub(Duration::hours(1));
    }

    let mut cursor_frequent = now.duration_round(Duration::minutes(15)).unwrap();
    for _ in 0..spec.frequently {
        wanted.push(cursor_frequent);
        cursor_frequent = cursor_frequent.sub(Duration::minutes(15));
    }

    wanted.sort_by(|a, b| b.cmp(a));

    let mut out = PruningPlan {
        keep: vec![],
        remove: vec![],
    };

    for snapshot in snapshots {
        let mut keep = false;
        if wanted.is_empty() {
            break;
        }
        while !wanted.is_empty() && &snapshot.time > wanted.last().unwrap() {
            wanted.pop().unwrap();
            keep = true;
        }
        if keep {
            out.keep.push(snapshot);
        } else {
            out.remove.push(snapshot);
        }
    }

    out
}

fn send_nonincremental(origin: &Remote, destination: &Remote, name: &str) {
    let mut snapshots = origin.list_snapshots();
    snapshots.sort_by(|a, b| a.time.cmp(&b.time));
    let path = &snapshots.last().unwrap().path;

    println!("Sending...");

    let mut producer = origin
        .cmd(&["send", "-w", path])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let consumer = destination
        .cmd(&["recv", "-uF", &destination.dataset])
        .stdin(producer.stdout.take().unwrap())
        .spawn()
        .unwrap();

    consumer.wait_with_output().unwrap();

    let bookmark = path.replace('@', "#") + &format!("-sync-{}", name);
    println!("Creating bookmark {}.", bookmark);
    origin.bookmark(&path, &bookmark);

    println!("Done.");
}

fn parse_spec(input: &str) -> Spec {
    let mut buf = String::new();
    let mut out = Spec {
        monthly: 0,
        weekly: 0,
        daily: 0,
        hourly: 0,
        frequently: 0,
    };
    for ch in input.chars() {
        if ('0'..='9').contains(&ch) {
            buf.push(ch);
            continue;
        }
        let num = buf.parse::<u64>().unwrap();
        buf = String::new();

        match ch {
            'm' => out.monthly = num,
            'w' => out.weekly = num,
            'd' => out.daily = num,
            'h' => out.hourly = num,
            'f' => out.frequently = num,
            _ => panic!("unrecognized duration {}", ch),
        }
    }
    if out.monthly == 0
        && out.weekly == 0
        && out.daily == 0
        && out.hourly == 0
        && out.frequently == 0
    {
        panic!("Cowardly refusing to keep nothing.");
    }
    out
}

fn parse_remote(input: &str) -> Remote {
    let indicies = input.rmatch_indices(':').collect::<Vec<_>>();
    if let Some((idx, _)) = indicies.first() {
        Remote {
            dataset: input[idx + 1..].to_string(),
            transport: Transport::SSH(input[0..*idx].to_string()),
        }
    } else {
        Remote {
            dataset: input.to_string(),
            transport: Transport::Local,
        }
    }
}

fn main() {
    let app = App::parse();

    let now = chrono::Utc::now();
    match app.subcmd {
        Subcommand::Snap(cmd) => {
            let origin = parse_remote(&cmd.location);

            let snapshots = origin.list_snapshots();
            let should_snapshot = if let Some(last) = snapshots.last() { 
                now.sub(last.time) > chrono::Duration::minutes(14)
            } else {
                true
            };
            if should_snapshot {
                let now_tag = now.format("%Y-%m-%dT%H%M");
                let path = format!("{}@{}", origin.dataset, now_tag);
                println!("Creating snapshot {}.", path);
                origin.snapshot(&path);
            }

            let spec = parse_spec(&cmd.keep);

            let snapshots = origin.list_snapshots();
            let prunable = find_prunable(&now, &spec, snapshots).remove;
            for snapshot in prunable {
                println!("Removing {}.", snapshot.path);
                origin.destroy_snapshot(&snapshot.path);
            }
        }
        Subcommand::Send(cmd) => {
            let destination_spec = parse_spec(&cmd.keep);

            let origin = parse_remote(&cmd.from);
            let destination = parse_remote(&cmd.to);

            let mut origin_bookmarks = origin.list_bookmarks(&cmd.name);
            origin_bookmarks.sort_by(|a, b| a.time.cmp(&b.time));

            let bookmark = match origin_bookmarks.last() {
                Some(x) => x,
                None => {
                    send_nonincremental(&origin, &destination, &cmd.name);
                    return;
                }
            };

            println!("Using bookmark {}.", bookmark.path);

            let mut snapshots_to_send = {
                let new_origin_snapshots = origin
                    .list_snapshots()
                    .into_iter()
                    .filter(|x| x.time > bookmark.time)
                    .collect::<Vec<_>>();
                find_prunable(&now, &destination_spec, new_origin_snapshots)
                    .keep
                    .into_iter()
                    .filter(|x| is_normal_snapshot(&x.path))
                    .collect::<Vec<_>>()
            };

            snapshots_to_send.sort_by(|a, b| a.time.cmp(&b.time));

            if snapshots_to_send.is_empty() {
                println!("Nothing to send.");
                return;
            }

            let dest_snapshots = destination.list_snapshots();
            for snapshot in dest_snapshots.iter().filter(|x| x.time > bookmark.time) {
                println!("Destroying destination's {}.", snapshot.path);
                destination.destroy_snapshot(&snapshot.path);
            }

            let send_paths = snapshots_to_send
                .into_iter()
                .map(|x| x.path)
                .collect::<Vec<_>>();

            println!("Sending:");
            for path in &send_paths {
                println!("- {}", path);
            }

            let mut first = true;
            let mut prev = bookmark.path.clone();
            for path in send_paths {
                println!("Sending {} -> {}.", prev, path);

                let flags = if first { "-wi" } else { "-wI" };

                let mut producer = origin
                    .cmd(&["send", flags, &prev, &path])
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap();

                let consumer = destination
                    .cmd(&["recv", "-u", &destination.dataset])
                    .stdin(producer.stdout.take().unwrap())
                    .spawn()
                    .unwrap();

                let out_consumer = consumer.wait_with_output().unwrap();
                if !out_consumer.status.success() {
                    println!("Error: {:?}", out_consumer);
                    return;
                }

                let out_producer = producer.wait_with_output().unwrap();
                if !out_producer.status.success() {
                    println!("Error: {:?}", out_producer);
                    return;
                }

                origin.bookmark(&path, &(path.replace('@', "#") + "-sync-" + &cmd.name));

                prev = path;
                first = false;
            }

            let mut origin_bookmarks = origin.list_bookmarks(&cmd.name);
            origin_bookmarks.sort_by(|a,b| a.time.cmp(&b.time));
            origin_bookmarks.pop(); // remove latest bookmark
            for bookmark in origin_bookmarks {
                println!("Pruning origin's bookmark {}", bookmark.path);
                origin.destroy_bookmark(&bookmark.path);
            }

            let destination_snapshots = destination.list_snapshots();
            let destination_plan = find_prunable(&now, &destination_spec, destination_snapshots);
            for snapshot in destination_plan.remove {
                println!("Pruning remote's snapshot {}", snapshot.path);
                destination.destroy_snapshot(&snapshot.path);
            }

            println!("Done.");
        }
    }
}
