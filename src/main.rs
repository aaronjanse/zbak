use chrono::{Datelike, Duration, DurationRound, TimeZone, Utc};
use clap::Clap;
use std::{
    collections::HashSet,
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

    fn exec(&self, args: &[&str]) -> Option<String> {
        let out = self.cmd(args).output().unwrap();
        if out.status.success() {
            Some(String::from_utf8(out.stdout).unwrap())
        } else {
            println!("err: {:?}", out);
            None
        }
    }

    fn internal_list_snapshots(&self) -> Option<Vec<Snapshot>> {
        let out = self.exec(&[
            "list",
            "-t",
            "snapshot",
            "-o",
            "name,creation",
            "-Hp",
            &self.dataset,
        ])?;

        Some(
            out.lines()
                .map(|line| {
                    let parts = line.split('\t').collect::<Vec<_>>();
                    Snapshot {
                        path: parts[0].to_string(),
                        time: chrono::Utc.timestamp(parts[1].parse::<i64>().unwrap(), 0),
                    }
                })
                .collect(),
        )
    }

    fn list_snapshots(&self) -> Vec<Snapshot> {
        let re = regex::Regex::new(r"^[a-z/]+@\d{4}-\d{2}-\d{2}T\d{4}$").unwrap();
        self.internal_list_snapshots()
            .unwrap()
            .into_iter()
            .filter(|snap| re.is_match(&snap.path))
            .collect()
    }

    fn list_markers(&self, name: &str) -> Vec<Snapshot> {
        let re = regex::Regex::new(r"^[a-z/]+@repl-").unwrap();
        match self.internal_list_snapshots() {
            Some(snaps) => snaps
                .into_iter()
                .filter(|snap| {
                    re.is_match(&snap.path) && snap.path.contains(&format!("@repl-{}", name))
                })
                .collect(),
            None => vec![],
        }
    }

    fn snapshot(&self, path: &str) {
        self.exec(&["snapshot", path]).unwrap();
    }

    fn destroy_snapshot(&self, path: &str) {
        if !path.contains('@') {
            panic!("invalid path for snapshot");
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

fn setup_replication(origin: &Remote, destination: &Remote, path: &str) {
    println!("Initial replication...");

    println!("Creating marker {}.", path);
    origin.snapshot(&path);

    let mut producer = origin
        .cmd(&["send", "-w", &path])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let consumer = destination
        .cmd(&["recv", "-uF", &destination.dataset])
        .stdin(producer.stdout.take().unwrap())
        .spawn()
        .unwrap();

    consumer.wait_with_output().unwrap();

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
            if let Some(last) = snapshots.last() {
                if now.sub(last.time) >= chrono::Duration::minutes(15) {
                    let now_tag = now.format("%Y-%m-%dT%H%M");
                    let path = format!("{}@{}", origin.dataset, now_tag);
                    println!("Creating snapshot {}.", path);
                    origin.snapshot(&path);
                }
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

            let now = chrono::Utc::now();
            let now_tag = now.format("%Y-%m-%dT%H%M%S");
            let path = format!("{}@repl-{}-{}", origin.dataset, cmd.name, now_tag);

            let markers_to_tags = |markers: &[Snapshot]| {
                markers
                    .iter()
                    .map(|snap| snap.path.split('@').nth(1).unwrap().to_string())
                    .collect::<HashSet<_>>()
            };

            let mut origin_markers = origin.list_markers(&cmd.name);
            let origin_tags = markers_to_tags(&origin_markers);

            let destination_markers = destination.list_markers(&cmd.name);
            let destination_tags = markers_to_tags(&destination_markers);

            let common_tags = origin_tags.intersection(&destination_tags);

            origin_markers.sort_by(|a, b| b.time.cmp(&a.time)); // recent, ..., old

            let mut common_marker = None;
            'search: for marker in &origin_markers {
                for tag in common_tags.clone() {
                    if marker.path.contains(tag) {
                        common_marker = Some(marker);
                        break 'search;
                    }
                }
            }

            if let Some(common) = common_marker {
                println!("Starting at {}.", common.path);

                let origin_snapshots = origin.list_snapshots();
                let mut destination_snapshots = destination.list_snapshots();

                let new_origin_snaps = origin_snapshots
                    .iter()
                    .cloned()
                    .filter(|snap| snap.time > common.time)
                    .collect::<Vec<_>>();

                let mut all_known_snapshots = vec![];
                all_known_snapshots.append(&mut new_origin_snaps.clone());
                all_known_snapshots.append(&mut destination_snapshots);

                let plan = find_prunable(&now, &destination_spec, all_known_snapshots);
                let keep_paths = plan.keep.iter().map(|x| x.path.clone()).collect::<Vec<_>>();
                let mut snapshots_to_send = new_origin_snaps
                    .into_iter()
                    .filter(|snap| keep_paths.contains(&snap.path))
                    .collect::<Vec<_>>();

                if snapshots_to_send.is_empty() {
                    println!("Nothing to send.");
                    return;
                }

                println!("Creating marker {}.", path);
                origin.snapshot(&path);

                snapshots_to_send.sort_by(|a, b| a.time.cmp(&b.time));

                let mut send_paths = snapshots_to_send
                    .into_iter()
                    .map(|x| x.path)
                    .collect::<Vec<_>>();
                send_paths.push(path);

                println!("Sending:");
                for path in &send_paths {
                    println!("- {}", path);
                }

                let mut prev = common.path.clone();
                for path in send_paths {
                    println!("Sending {} -> {}.", prev, path);

                    let mut producer = origin
                        .cmd(&["send", "-wI", &prev, &path])
                        .stdout(Stdio::piped())
                        .spawn()
                        .unwrap();

                    let consumer = destination
                        .cmd(&["recv", "-u", &destination.dataset])
                        .stdin(producer.stdout.take().unwrap())
                        .spawn()
                        .unwrap();

                    consumer.wait_with_output().unwrap();

                    prev = path;
                }

                println!("Pruning origin markers.");
                for marker in origin_markers {
                    origin.destroy_snapshot(&marker.path);
                }

                println!("Pruning destination markers.");
                for marker in destination_markers {
                    destination.destroy_snapshot(&marker.path);
                }

                let snapshots_to_prune = destination_snapshots
                    .into_iter()
                    .filter(|snap| plan.remove.contains(&snap))
                    .collect::<Vec<_>>();

                println!("Pruning destination snapshots.");
                for snapshot in snapshots_to_prune {
                    println!("Removing {}.", snapshot.path);
                    destination.destroy_snapshot(&snapshot.path);
                }

                println!("Done.");
            } else {
                setup_replication(&origin, &destination, &path);
            }
        }
    }
}
