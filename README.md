# dispatch

Sebuah job dispatch engine murni Go: priority queue dengan semantik pengiriman
bergaya SQS, worker pool berbatas, rate limiter yang bisa dikomposisi, dan event
bus yang dirutekan berdasarkan topik.

**Tanpa dependensi eksternal.** Semuanya — termasuk setiap test — dibangun di atas
standard library. Tidak ada `go.sum`.

```
git clone https://github.com/Inc-cryp/go-dispatch
cd go-dispatch
go test -race ./...
go run ./cmd/dispatchd -subjects 2000
```

Lalu di shell lain:

```
curl localhost:8080/healthz
curl -s localhost:8080/stats | python3 -m json.tool
```

---

## Kenapa ini ada

Job queue adalah salah satu hal yang terlihat sepele sampai ternyata tidak. Bagian
yang menarik bukan "taruh sebuah struct ke dalam channel" — melainkan semua yang
terjadi ketika seorang worker mati di tengah job, ketika layanan hilir
me-rate-limit Anda, ketika proses menerima `SIGTERM` dengan 400 job sedang
melayang, dan ketika pemanggil yang sedang polling shutdown tanpa sengaja memegang
lock yang memblokir semua producer.

Ini adalah implementasi berskala portofolio untuk masalah tersebut, ditulis agar
enak dibaca. Setiap package berdiri sendiri, bisa di-test secara independen, dan
bebas dari goroutine latar belakang yang tidak Anda minta.

### Apa yang jujur dari desain ini

Ini adalah queue **single-node dan in-memory**. Repo ini tidak mengklaim
sebaliknya. Versi durable dan terdistribusi adalah program yang berbeda: ia
membutuhkan replicated log, fencing token, dan consumer yang idempotent. Yang
didemonstrasikan repo ini adalah *semantik dan mesin konkurensinya* — pengiriman
at-least-once, visibility timeout, exponential backoff, dead-lettering,
backpressure, graceful drain — di balik interface yang cukup sempit sehingga
backend SQS, Redis, atau Postgres bisa ditukar tanpa menyentuh worker pool.

---

## Arsitektur

```mermaid
flowchart LR
    P["Producer<br/><i>Enqueue(Entry)</i>"] --> Q

    subgraph Q["queue.Queue — satu goroutine scheduler"]
        H["priority heap<br/>min-heap by (priority, readyAt, seq)"]
        V["reservation table<br/>visibility deadline per delivery"]
        D["dead-letter list<br/>capped, ring-buffered IDs"]
    end

    Q -->|"Dequeue(ctx)<br/>reserves + arms visibility timer"| W

    subgraph W["worker.Pool"]
        L{"ratelimit.Limiter<br/>opsional"}
        L --> W1["worker 1"]
        L --> W2["worker 2"]
        L --> WN["worker N"]
    end

    W -->|"Ack"| Q
    W -->|"Nack(err)<br/>→ retry with backoff<br/>→ or dead-letter"| Q

    Q -.->|"TopicEnqueued / Dequeued /<br/>Done / Failed / Retried / Requeued"| B["queue.Subscription"]
    B -->|"bridged in cmd/dispatchd"| EB["eventbus.Bus<br/>pattern-matched topics"]

    EB --> S1["job.> handler"]
    EB --> S2["job.failed handler"]
    EB --> SN["..."]
```

Graf dependensinya berlapis secara ketat, tanpa siklus:

| Package | Import (internal) | Peran |
| --- | --- | --- |
| `queue` | — | Penyimpanan job yang mendekati durable, semantik pengiriman, retry |
| `ratelimit` | — | Token bucket, fixed window, `Multi`, `Keyed` per-key |
| `eventbus` | — | Pub/sub dengan pola topik dan drop policy per subscriber |
| `worker` | `queue`, `ratelimit` | Pool berbatas; memanggil interface `Sink` |
| `cmd/dispatchd` | keempatnya | Wiring, HTTP health/stats, penanganan signal |

`queue`, `ratelimit`, dan `eventbus` tidak tahu apa-apa satu sama lain. `worker`
tidak pernah meng-import `eventbus` — jembatan antara event stream milik queue dan
bus berada di `cmd/dispatchd`, sehingga tidak ada package yang menumbuhkan
dependensi yang tidak dibutuhkannya.

---

## Keputusan desain yang layak dipertahankan

Ini adalah pilihan-pilihan yang benar-benar akan dipertanyakan seorang reviewer.
Masing-masing adalah trade-off, bukan kebetulan.

### Queue memelihara tepat satu goroutine

`queue.New` menjalankan satu goroutine scheduler. Ia satu-satunya yang mengubah
delayed-delivery dan visibility state. Producer dan consumer mengambil mutex untuk
menyentuh heap, tetapi *waktu* hanya milik scheduler.

Alternatifnya — `time.AfterFunc` per job — tidak akan bertahan saat berhadapan
dengan sejuta job. Ia juga membuat shutdown menjadi non-deterministik: tidak ada
cara mengetahui kapan semua timer yang tertunda sudah selesai menyala. Satu
goroutine dengan `time.Timer` yang di-reset ke deadline berikutnya itu
prediktabel, murah, dan mudah di-drain.

`Queue.Close()` bersifat idempotent dan aman konkuren; ia menghentikan scheduler,
menutup setiap subscription, lalu menandai queue tertutup. Enqueue setelah itu
mengembalikan `ErrClosed`, bukan panic.

### Fan-out event queue: topic disaring, subscriber lambat dibuang

`Subscribe(topics...)` menyaring per topic, dan `Subscribe()` tanpa argumen berarti
semuanya. Setiap transisi memancarkan tepat satu event: `enqueued`, `dequeued`,
`done`, `failed`, `retried`, dan `requeued`, masing-masing membawa `JobID`, `State`,
`Attempt`, dan `At` — cukup bagi operator untuk merekonstruksi riwayat sebuah job
tanpa menyentuh queue-nya.

Pengirimannya **non-blocking**: subscriber yang buffernya penuh kehilangan event
dan menghitungnya di `Subscription.Dropped()`, alih-alih menahan queue. Liveness
queue mengalahkan kelengkapan notifikasi, dan notifikasi yang hilang bisa dipulihkan
dari `Stats()`. `Close` pada subscription idempotent dan aman dipanggil bersamaan
dengan producer yang sedang mengirim.

### Visibility timeout, bukan lock yang dipegang sepanjang handler

`Dequeue` tidak menyerahkan job lalu melupakannya. Ia mencatat reservation dengan
sebuah deadline dan mempersenjatai scheduler. Worker harus memanggil `Ack`
(selesai) atau `Nack` (retry atau dead-letter) sebelum deadline itu. Jika prosesnya
dibunuh, reservation-nya hanya kedaluwarsa dan job kembali siap — inilah yang
membuat pengirimannya at-least-once, bukan at-most-once.

Reservation yang kedaluwarsa sengaja **tidak** memakan satu attempt. Worker yang
OOM-killed tidak menggagalkan job; menghukum job karena kegagalan infrastruktur
akan diam-diam menghabiskan `MaxAttempts` selama crash loop.

### Prioritas adalah (priority, readyAt, sequence) — dan sentinel-nya penting

Job yang sifatnya immediate dinormalkan ke `RunAt` bernilai nol, bukan
`time.Now()`. Ini dulunya bug nyata: ketika setiap entry membawa `time.Now()`
miliknya sendiri, timestamp itu menjadi kunci urutan kedua, sehingga ia selalu
menentukan ordering dan field priority menjadi dead code. Menormalkan ke zero time
membuat prioritas dan urutan penyisipan yang menentukan, dan hanya job *tertunda*
yang diurutkan berdasarkan wall clock.

### `DefaultBackoff` menerapkan cap setelah jitter

```go
delay := base * (1 << (attempt - 1))   // exponential
delay += jitter(delay)                 // decorrelate retries
if delay > cap { delay = cap }         // cap LAST
```

Memberi cap sebelum menambahkan jitter berarti cap bisa terlampaui, dan itu
menggagalkan tujuan cap itu sendiri. Ini juga bug nyata yang ditemukan test suite.

### `Publish` adalah fan-out sinkron; handler-nya asinkron

`eventbus.Publish` mencocokkan subscription dan mengirim ke buffered channel milik
setiap subscriber *sebelum kembali*. Eksekusi handler berjalan di luar goroutine
publisher. Ini memberi publisher biaya yang terbatas dan jujur: satu pengiriman
channel per subscriber yang cocok.

Tetapi artinya ada satu hal penting: buffer subscriber yang penuh tidak boleh
sampai memblokir publisher. Ketika buffer penuh, `DropPolicy` milik subscriber
yang memutuskan:

- `DropNewest` (default) — publisher tidak pernah terblokir; event-nya dibuang dan
  dihitung di `Subscription.Dropped()`. **Subscriber yang lambat tidak bisa
  memperlambat Anda.**
- `Block` — publisher menunggu sampai ada ruang, dibatasi context.

Default-nya `DropNewest` justru karena mode kegagalan dari `Block` adalah consumer
lambat yang secara misterius menahan producer lain yang tidak berhubungan.
Membuang itu berisik (`Dropped()` adalah counter, bukan baris log) dan bersifat
lokal.

### `matching()` melepas lock bus sebelum mengirim

Di bawah `Block`, pengiriman channel bisa menunggu tanpa batas. Memegang lock bus
sepanjang pengiriman itu akan membuat satu subscriber lambat menahan *setiap*
publisher di bus. Karena itu kumpulan subscriber yang cocok dikumpulkan di bawah
lock, lalu lock-nya dilepas sebelum pengiriman mana pun terjadi.

### Rate limiter tidak punya goroutine latar belakang

Token bucket mengisi ulang secara lazy: setiap panggilan menghitung waktu yang
berlalu dan menambahkan token yang sesuai. Tanpa goroutine, tanpa timer per
limiter, biaya sebanding dengan pemakaian sebenarnya.

Setiap limiter menerima clock yang di-inject (`WithClock`). Test memajukan waktu
secara eksplisit alih-alih tidur, itulah sebabnya test suite berjalan dalam
hitungan detik alih-alih menit dan tidak flaky.

### `Keyed` sengaja **tidak** mengimplementasikan `Limiter`

`TokenBucket`, `FixedWindow`, dan `Multi` adalah `Limiter` — mereka tidak menerima
key. `Keyed` memerlukan satu, jadi method-nya adalah `Allow(key)`, `Reserve(key)`,
`Wait(ctx, key)`.

Ini keputusan desain yang dikunci oleh sebuah test (`TestKeyedIsNotALimiter`).
Godaannya adalah menambahkan stub tanpa argumen agar `Keyed` memenuhi `Limiter`
dan bisa dilempar ke mana saja. Tapi stub `Wait` yang mengembalikan `nil` membuat
pemanggil tanpa key gagal secara **fail-open** — ia diam-diam tidak akan pernah
melakukan throttling apa pun. Membuat signature-nya tidak kompatibel mengubah bug
produksi yang senyap menjadi compile error.

### `Wait` memeriksa context sebelum menghabiskan token

`TokenBucket.Wait` pada context yang sudah dibatalkan mengembalikan `ctx.Err()`
tanpa menghabiskan kapasitas. Ini bug fail-open yang ditemukan saat proses
verifikasi: loop-nya hanya memeriksa `ctx.Err()` di jalur sleep, sehingga iterasi
pertama akan menghabiskan satu token lalu melaporkan sukses pada context yang
sudah mati. Kedua implementasi `Wait` sekarang memeriksa di awal setiap iterasi
dan sekali lagi setelah memberikan token.

### Worker pool memisahkan context-nya menjadi dua

`Pool.Start(ctx)` menurunkan sebuah context *dispatch* (bisa dibatalkan, menggerbangi
`Dequeue` dan penantian limiter) tetapi meneruskan `ctx` asli milik pemanggil ke
handler. `Shutdown` yang graceful hanya membatalkan context dispatch, sehingga job
yang sedang berjalan selesai sampai tuntas alih-alih dibatalkan di tengah handler.

Karena context handler adalah `ctx` itu sendiri, pemanggil tidak boleh menyerahkan
context yang mati saat `Shutdown` berjalan. `cmd/dispatchd` menurunkan context
handler dari context `SIGTERM` lewat `context.WithoutCancel`: nilainya tetap
terbawa, tetapi pembatalan sinyal tidak merambat ke job yang sedang melayang.

`Shutdown(ctx)` lalu melakukan drain: ia berhenti melakukan dequeue, menunggu job
yang masih melayang sampai `DrainTimeout`, dan saat kedaluwarsa mengembalikan error
yang membungkus `context.DeadlineExceeded` beserta jumlah job yang masih melayang.
Ia idempotent — memanggilnya dua kali aman dan panggilan kedua langsung kembali.

### `worker.New` panic pada sink atau handler yang nil

Handler nil dulunya diam-diam diganti dengan no-op, yang berarti setiap job
"berhasil" sambil tidak melakukan apa pun: kehilangan data total yang senyap.
Kesalahan pemrograman pada waktu konstruksi seharusnya berisik. `New` panic dengan
pesan yang eksplisit; context `Start(nil)` mengembalikan `ErrNilContext` alih-alih
panic di dalam `context.WithCancel`.

### Error adalah nilai, dan ia dibungkus

Sentinel (`queue.ErrClosed`, `worker.ErrAlreadyStarted`, bebas `ratelimit` secara
desain, `eventbus.ErrBusClosed`, ...) diperiksa dengan `errors.Is`. Error yang
dibungkus memakai `%w`. Handler yang panic dilaporkan sebagai `ErrHandlerPanic`
yang dibungkus bersama job ID, sehingga handler yang bermasalah bisa dibedakan
dari job yang gagal secara sah di hilir.

### Interface didefinisikan di tempat ia dikonsumsi

`worker.Sink` adalah subset empat method dari `*queue.Queue` yang benar-benar
dibutuhkan pool (`Dequeue`, `Ack`, `Nack`, `Extend`). `cmd/dispatchd` meneruskan
queue asli; test meneruskan fake yang bisa diskenariokan. Inilah peribahasa Go yang
diwujudkan: consumer-lah yang mendeklarasikan interface, sehingga pool bisa
di-test tanpa queue sama sekali.

### Pool menjalankan sekumpulan goroutine yang eksplisit

`Start` meluncurkan sejumlah goroutine worker yang tetap (`WithWorkers`) plus
sebuah dispatcher yang memanggil `Dequeue` dan menyerahkan setiap delivery ke
worker yang kosong lewat buffered channel. Worker tidak pernah menyentuh
reservation secara langsung — mereka melaporkan hasilnya dan jalur pelepasan yang
melakukan ack atau nack. Menjaga jumlah goroutine yang memanggil ke dalam queue
tetap kecil dan berbatas adalah yang membuat locking di queue itu sendiri
prediktabel.

`InFlight()` dan `Stats()` dilayani dari atomic dan panjang channel, sehingga
mengamati pool yang sedang berjalan tidak pernah memblokirnya.

---

## Benchmark

Diukur, bukan diperkirakan. Setiap angka di bawah berasal dari
`go test -bench . -benchtime 100ms` pada **Apple M1 (8 core), go1.26.0,
darwin/arm64**. Reproduksi dengan:

```
go test -run XXX -bench . -benchtime 100ms ./...
```

### `queue`

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `Enqueue` | 411.8 | 289 | 4 |
| `EnqueueParallel` | 575.0 | — | — |
| `EnqueueDequeueAck` (siklus penuh) | 562.4 | 0 | 0 |
| `DequeuePriority` | 1030 | 0 | 0 |
| `DequeueParallel` | 751.5 | 0 | — |
| `SubscribeFanout/subscribers=1` | 609.0 | 278 | 3 |
| `SubscribeFanout/subscribers=4` | 1047 | 311 | 3 |
| `SubscribeFanout/subscribers=16` | 1357 | 407 | 4 |

Steady state yang nol alokasi itulah intinya: setelah `Enqueue` mengalokasikan
entry-nya, siklus dequeue/ack mendaur ulang dan tidak mengalokasikan apa pun.

### `eventbus`

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `Publish` | 245.7 | 80 | 3 |
| `PublishParallel` | 247.4 | 56 | 3 |
| `PublishNoMatch` | 290.2 | 223 | 7 |
| `PublishSync` | 155.3 | 79 | 3 |
| `HandlerThroughput` (end-to-end) | 364.7 | 79 | 3 |
| `PublishDropNewest` (jalur overflow) | 150.3 | 79 | 3 |
| `MatchTopic/job.created` | 69.08 | 64 | 2 |
| `MatchTopic/job.*` | 66.51 | 64 | 2 |
| `MatchTopic/job.>` | 66.38 | 64 | 2 |
| `MatchTopic/>` | 55.70 | 48 | 2 |
| `SubscribeFanout/subscribers=1` | 255.0 | 79 | 3 |
| `SubscribeFanout/subscribers=8` | 1925 | 583 | 17 |
| `SubscribeFanout/subscribers=32` | 6417 | 2311 | 65 |

Dua hal menonjol. Jalur overflow (150 ns/op) justru *lebih murah* daripada jalur
pengiriman (246 ns/op), dan memang begitulah bentuk `DropNewest` yang seharusnya —
admission control sedang menjalankan tugasnya. Dan fan-out-nya linear: ~200 ns per
subscriber, sehingga biaya broadcast-nya terprediksi, bukan kuadratik.

### `ratelimit`

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `TokenBucketAllow` (ditolak) | 30.13 | 0 | 0 |
| `TokenBucketAllowGranted` | 39.05 | 0 | 0 |
| `TokenBucketAllowParallel` | 120.9 | 0 | 0 |
| `TokenBucketReserve` | 29.12 | 0 | 0 |
| `FixedWindowAllow` | 29.66 | 0 | 0 |
| `FixedWindowAllowParallel` | 126.6 | 0 | 0 |
| `MultiAllow/limiters=1` | 29.47 | 0 | 0 |
| `MultiAllow/limiters=2` | 37.29 | 0 | 0 |
| `MultiAllow/limiters=4` | 108.1 | 0 | 0 |
| `MultiAllow/limiters=8` | 213.9 | 0 | 0 |
| `KeyedGet` | 17.70 | 0 | 0 |
| `KeyedGetParallel` | 186.2 | 14 | 1 |
| `KeyedEviction` (kasus terburuk LRU) | 183.9 | 168 | 5 |
| `WaitImmediate` (jalur cepat) | 72.84 | 0 | 0 |

Seluruh limiter ini bebas alokasi di jalur panasnya. Penolakan sama murahnya
dengan pemberian, jadi limiter yang sedang menolak trafik bukanlah beban — dan itu
penting, karena limiter biasanya berjalan pada momen paling diperebutkan justru
ketika ia sedang melakukan throttling. `KeyedGet` pada 17.7 ns adalah map hit
biasa; `KeyedEviction` membayar 168 B untuk limiter baru, dan itulah biaya jujur
dari memori yang berbatas.

### `worker`

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `PoolThroughput/workers=1` | 479.6 | 0 | 0 |
| `PoolThroughput/workers=4` | 396.3 | 0 | 0 |
| `PoolThroughput/workers=16` | 510.4 | 0 | 0 |
| `PoolWithLimiter` | 502.7 | 0 | 0 |
| `ResultCallback/disabled` | 416.5 | 0 | 0 |
| `ResultCallback/enabled` | 499.3 | 0 | 0 |
| `PoolStartShutdown` | 4709 | 3089 | 25 |

Jujur soal bentuknya di sini: throughput-nya ~400–500 ns/job dan **tidak membaik
melewati 4 worker** pada M1 8-core. Itu karena sink benchmark-nya (satu buffered
channel) yang menjadi bottleneck, bukan pool-nya — nilai yang terukur adalah
overhead scheduling plus serah-terima channel, jadi ini lantai, bukan langit-langit.
Handler no-op tidak memakan biaya apa pun, dan itulah sinyal yang dimaksudkan.

`PoolStartShutdown` pada 4.7 µs / 25 alokasi memberi tahu Anda bahwa pool berumur
pendek cukup murah untuk dibuat per-test atau per-batch request.

---

## Apa yang dicakup test

```
$ go test -race -count=1 ./...
ok  github.com/Inc-cryp/go-dispatch/cmd/dispatchd
ok  github.com/Inc-cryp/go-dispatch/eventbus
ok  github.com/Inc-cryp/go-dispatch/queue
ok  github.com/Inc-cryp/go-dispatch/ratelimit
ok  github.com/Inc-cryp/go-dispatch/worker
```

Setiap package dijalankan dengan `-race`. Di luar test fitur, setiap package punya
`leak_test.go` yang mengambil snapshot `runtime.NumGoroutine()`, mengocok sumber
daya package tersebut (queue close, siklus subscribe/close bus, start/shutdown
pool, key churn limiter), lalu menegaskan jumlahnya kembali ke baseline —
mengulang hingga 5 detik dan mencetak seluruh `runtime.Stack` saat gagal. Goroutine
yang bocor adalah mode kegagalan yang baru muncul di produksi berbulan-bulan
kemudian, jadi ia mendapat test eksplisit alih-alih sekadar komentar.

Baris-baris di atas bukan tempelan: `.github/workflows/ci.yml` menjalankan suite
yang sama dengan `-race`, ditambah `make lint` (`fmt-check`, `vet`, `staticcheck`,
`golangci-lint`) dan `make smoke` — yang membangun `dispatchd`, menjalankannya, lalu
memeriksa `/healthz` dan `/stats` benar-benar menjawab. Pipeline-nya tidak
mengimplementasi ulang target apa pun dalam YAML; ia memanggil `make`, sehingga CI
dan mesin lokal Anda tidak bisa berbeda pendapat soal arti "lulus" — dan seluruh
gate itu bisa Anda reproduksi di laptop dengan dua perintah: `make check` dan
`make smoke`.

Beberapa perilaku spesifik yang dikunci oleh test:

- `TestWaitWithAlreadyCancelledContextDoesNotConsume` — regresi fail-open tadi.
- `TestKeyedIsNotALimiter` — pilihan type-system di atas.
- `TestServeDrainsInFlightHandlersOnSignal` — `serve` menyerahkan context ke
  `Start`, bukan ke `Shutdown`. Selama wiring-nya salah, `SIGTERM` membatalkan
  handler yang justru sedang ditunggu drain, dan job yang melayang kembali
  sebagai "handler interrupted by shutdown".
- `TestDequeueErrorDoesNotRunTheHandlerOnAPhantomJob` — `Dequeue` yang gagal
  tidak pernah menghasilkan delivery, jadi handler tidak boleh dipanggil dengan
  `Delivery` bernilai nol.
- Urutan prioritas versus FIFO untuk prioritas yang sama, termasuk job tertunda.
- Reservation yang kedaluwarsa tidak memakan satu attempt.
- `Nack` mengembalikan `ErrRetryScheduled` saat akan retry, `ErrJobFailed` saat
  dead-letter.
- `Shutdown` pada pool yang sudah berhenti, dan `Close` yang dipanggil dua kali
  pada setiap tipe.
- Handler yang panic menjadi `ErrHandlerPanic` alih-alih membunuh pool.
- `Publish` di bawah policy `Block` terbuka saat bus ditutup.

---

## Struktur project

```
go-dispatch/
├── cmd/dispatchd/        demo service: wiring, HTTP, penanganan signal
├── docs/DESIGN.md        catatan desain mendalam dan narasi trade-off
├── eventbus/             pub/sub dengan pola topik
├── queue/                priority queue + semantik pengiriman
├── ratelimit/            token bucket, fixed window, Multi, Keyed
└── worker/               worker pool berbatas + graceful drain
```

README ini adalah tur singkatnya; [`docs/DESIGN.md`](docs/DESIGN.md) adalah bagian
dalamnya — invariant, taksonomi kegagalan, bug yang ditangkap test, dan apa yang
harus diubah untuk backend yang durable.

Total: ~6,700 baris termasuk test, tersebar di 30 file.

### Flag `dispatchd`

| Flag | Default | Arti |
| --- | --- | --- |
| `-workers` | `8` | Jumlah job runner konkuren |
| `-rate` | `200` | Maksimum job yang dimulai per detik (`0` menonaktifkan limiter) |
| `-burst` | `50` | Kapasitas burst limiter |
| `-addr` | `:8080` | Alamat HTTP untuk listen |
| `-subjects` | `2000` | Jumlah job yang digenerate saat start |
| `-shutdown-timeout` | `10s` | Batas waktu graceful drain |
| `-log-level` | `info` | `debug`, `info`, `warn`, `error` |
| `-healthcheck` | _(off)_ | Probe URL ini lalu keluar 0/1 alih-alih melayani |

`-healthcheck` ada karena image container-nya `FROM scratch` dan tidak memuat
`curl` maupun `wget` untuk dipanggil. Binary-nya memeriksa dirinya sendiri:

```
$ dispatchd -healthcheck=http://127.0.0.1:8080/healthz && echo alive
alive
```

Demo-nya menghasilkan empat jenis job — `email.send`, `image.resize`, `slow.index`,
dan `flaky.report` (yang gagal secara berkala, untuk menguji jalur retry dan
dead-letter). Setiap transisi queue dijembatani ke event bus dan dicatat.

Urutan shutdown-nya disengaja: hentikan listener HTTP, lalu drain pool agar tidak
ada job yang terpotong di tengah jalan, lalu tutup bus **terakhir** agar transisi
status terakhir tetap terpublikasikan.

---

## Roadmap

Interface-nya adalah bagian yang menarik; inilah backend-backend yang menjadi
alasan bentuknya seperti sekarang.

- [ ] **`queue.Backend`**: ekstrak penyimpanan ke balik sebuah interface lalu
      tambahkan implementasi Redis (delayed set + reservation berbasis lock) dan
      Postgres (`FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY`).
- [ ] **Idempotency key** agar consumer at-least-once bisa melakukan deduplikasi
      retry.
- [ ] **Fair scheduling**: queue per-tenant dengan weighted round-robin alih-alih
      satu priority heap global.
- [ ] **Adaptive visibility timeout**: perpanjang reservation secara otomatis saat
      handler melaporkan kemajuan, alih-alih bergantung pada `Extend` yang tetap.
- [ ] **Structured tracing**: propagasikan `traceparent` lewat metadata
      `Entry.Payload` agar retry sebuah job muncul sebagai satu trace.
- [ ] **Prometheus metrics exporter** untuk counter yang sudah diekspos `Stats()`.

---

## Lisensi

MIT — lihat [`LICENSE`](LICENSE). Singkatnya: pakai, ubah, dan jual sesuka Anda,
selama notice hak cipta dan izinnya ikut disertakan. Perangkat lunaknya datang
tanpa jaminan apa pun.
