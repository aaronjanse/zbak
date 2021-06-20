**Do not use this yet. It's still a work-in-progress, and I've barely tried it myself.**

`zbak` is a zfs snapshot and replication tool that's easy to use. 

```console
$ # locally keep 7 daily, 24 hourly, and 4 frequent (15-min) snapshots
$ zbak snap zroot/code --keep 7d24h4f
$ # remotely keep 6 monthly, 4 weekly, and 7 daily snapshots
$ zbak send --name rpi4 --from zroot/code --to rpi4.local:rpool/code --keep 6m4w7d
```
