# dispatch

Job dispatch engine murni Go: priority queue dengan semantik pengiriman ala SQS,
worker pool berbatas, rate limiter yang bisa dikomposisi, dan event bus ber-topik.

**Nol dependensi eksternal.** Semua — termasuk test — cuma pakai standard library.
Tidak ada `go.sum`.

```bash
git clone https://github.com/Inc-cryp/go-dispatch
cd go-dispatch
go test -race ./...
go run ./cmd/dispatchd -subjects 2000
```

Di shell lain:

```bash
curl localhost:8080/healthz
curl -s localhost:8080/stats | python3 -m json.tool
```

## Kenapa ini ada

Job queue kelihatan sepele sampai ternyata tidak. Bagian menariknya bukan "taruh
struct ke channel", tapi semua yang terjadi ketika: worker mati di tengah job,
layanan hilir me-rate-limit kamu, proses kena `SIGTERM` sementara 400 job masih
melayang, dan pemanggil yang lagi polling shutdown tanpa sengaja memegang lock
yang bikin semua producer menunggu.

**Ini queue single-node dan in-memory.** Repo ini tidak mengklaim sebaliknya.
Versi durable dan terdistribusi itu program yang beda: butuh replicated log,
fencing token, dan consumer yang idempoten. Yang didemonstrasikan di sini adalah
*semantik dan mesin konkurensinya* — pengiriman at-least-once, visibility timeout,
exponential backoff, dead-lettering, backpressure, graceful drain — di balik
interface yang cukup sempit sehingga backend SQS, Redis, atau Postgres bisa
ditukar tanpa menyentuh worker pool.

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

Dependensinya berlapis ketat, tanpa siklus:

| Package | Import (internal) | Peran |
| --- | --- | --- |
| `queue` | — | Penyimpanan job yang mendekati durable, semantik pengiriman, retry |
| `ratelimit` | — | Token bucket, fixed window, `Multi`, `Keyed` per-key |
| `eventbus` | — | Pub/sub dengan pola topik dan drop policy per subscriber |
| `worker` | `queue`, `ratelimit` | Pool berbatas; memanggil interface `Sink` |
| `cmd/dispatchd` | keempatnya | Wiring, HTTP health/stats, penanganan signal |

`queue`, `ratelimit`, dan `eventbus` tidak saling tahu. `worker` tidak pernah
meng-import `eventbus` — jembatan dari event stream queue ke bus ada di
`cmd/dispatchd`. Jadi tidak ada package yang menumbuhkan dependensi yang tidak
dibutuhkannya.

## Keputusan desain yang layak dipertahankan

Ini pilihan-pilihan yang bakal benar-benar ditanya reviewer. Semuanya trade-off,
bukan kebetulan.

### Queue cuma punya satu goroutine

`queue.New` menjalankan satu goroutine scheduler, dan cuma dia yang mengubah
delayed-delivery dan visibility state. Producer dan consumer ambil mutex buat
menyentuh heap, tapi *waktu* hanya milik scheduler.

Alternatifnya — `time.AfterFunc` per job — tidak akan bertahan menghadapi sejuta
job, dan bikin shutdown jadi non-deterministik: tidak ada cara tahu kapan semua
timer yang tertunda selesai menyala. Satu goroutine dengan `time.Timer` yang
di-reset ke deadline berikutnya itu prediktabel, murah, dan gampang di-drain.

`Queue.Close()` idempotent dan aman konkuren: ia menghentikan scheduler, menutup
setiap subscription, lalu menandai queue tertutup. Enqueue setelah itu
mengembalikan `ErrClosed`, bukan panic.

### Visibility timeout, bukan lock sepanjang handler

`Dequeue` tidak menyerahkan job lalu melupakannya. Ia mencatat reservation dengan
sebuah deadline dan mempersenjatai scheduler. Worker harus memanggil `Ack`
(selesai) atau `Nack` (retry atau dead-letter) sebelum deadline itu. Kalau
prosesnya dibunuh, reservation-nya cuma kedaluwarsa dan job kembali siap — inilah
yang membuat pengirimannya at-least-once, bukan at-most-once.

Reservation yang kedaluwarsa sengaja **tidak** memakan satu attempt. Worker yang
OOM-killed tidak menggagalkan job; menghukum job karena kegagalan infrastruktur
akan diam-diam menghabiskan `MaxAttempts` selama crash loop.

Konsekuensinya dulu job seperti itu dikirim ulang selamanya, jadi `Entry` punya
anggaran lapse yang terpisah dari attempt:

```go
q.Enqueue(queue.Entry{ID: "report", MaxAttempts: 3, MaxLapses: 5})
```

Setelah reservation kelima kedaluwarsa tanpa `Ack`/`Nack`, job di-dead-letter
dengan `ErrMaxLapses` dan `Stats().Lapsed` bertambah. Lapse tetap tidak memakan
attempt, jadi `MaxAttempts: 1` dengan `MaxLapses: 5` bukan kontradiksi: satu
attempt yang vonisnya tidak pernah dilaporkan boleh hilang lima kali. `MaxLapses`
bernilai nol (default) berarti tak terbatas dan mempertahankan perilaku lama.

### Prioritas itu (priority, readyAt, sequence) — dan sentinel-nya penting

Job yang immediate dinormalkan ke `RunAt` bernilai nol, bukan `time.Now()`. Ini
dulunya bug nyata: ketika setiap entry membawa `time.Now()` miliknya sendiri,
timestamp itu jadi kunci urutan kedua, sehingga ia selalu menentukan ordering dan
field priority berubah jadi dead code. Menormalkan ke zero time membuat prioritas
dan urutan penyisipan yang menentukan, dan hanya job *tertunda* yang diurutkan
berdasarkan wall clock.

### `DefaultBackoff` menerapkan cap setelah jitter

```go
delay := base * (1 << (attempt - 1))   // exponential
delay += jitter(delay)                 // decorrelate retries
if delay > cap { delay = cap }         // cap LAST
```

Memberi cap sebelum menambahkan jitter berarti cap-nya bisa terlampaui, dan itu
menggagalkan tujuan cap itu sendiri. Ini juga bug nyata yang ditemukan test suite.

### Event bus: fan-out sinkron, handler asinkron, subscriber lambat dibuang

`Publish` mencocokkan subscription dan mengirim ke buffered channel milik setiap
subscriber *sebelum kembali*; eksekusi handler-nya jalan di luar goroutine
publisher. Jadi biaya publisher terbatas dan jujur: satu pengiriman channel per
subscriber yang cocok.

Konsekuensinya, buffer subscriber yang penuh tidak boleh sampai memblokir
publisher. Di situ `DropPolicy` milik subscriber yang memutuskan:

- `DropNewest` (default) — publisher tidak pernah terblokir; event-nya dibuang dan
  dihitung di `Subscription.Dropped()`. **Subscriber lambat tidak bisa
  memperlambat kamu.**
- `Block` — publisher menunggu sampai ada ruang, dibatasi context.

Default-nya `DropNewest` justru karena mode kegagalan `Block` adalah consumer
lambat yang secara misterius menahan producer lain yang tidak berhubungan.
Membuang itu berisik (`Dropped()` itu counter, bukan baris log) dan lokal.

Karena di bawah `Block` pengiriman channel bisa menunggu tanpa batas,
`matching()` melepas lock bus sebelum mengirim. Kalau tidak, satu subscriber
lambat akan menahan *setiap* publisher di bus.

### Rate limiter tidak punya goroutine latar belakang

Token bucket mengisi ulang secara lazy: setiap panggilan menghitung waktu yang
berlalu dan menambahkan token yang sesuai. Tanpa goroutine, tanpa timer per
limiter, biaya sebanding dengan pemakaian sebenarnya.

Setiap limiter menerima clock yang di-inject (`WithClock`). Test memajukan waktu
secara eksplisit alih-alih tidur — itulah sebabnya suite-nya jalan dalam hitungan
detik, bukan menit, dan tidak flaky.

### `Keyed` sengaja **tidak** mengimplementasikan `Limiter`

`TokenBucket`, `FixedWindow`, dan `Multi` adalah `Limiter` — mereka tidak
menerima key. `Keyed` butuh satu, jadi method-nya `Allow(key)`, `Reserve(key)`,
`Wait(ctx, key)`.

Ini keputusan yang dikunci sebuah test (`TestKeyedIsNotALimiter`). Godaannya
adalah menambahkan stub tanpa argumen supaya `Keyed` memenuhi `Limiter` dan bisa
dilempar ke mana saja. Tapi stub `Wait` yang mengembalikan `nil` bikin pemanggil
tanpa key gagal secara **fail-open** — diam-diam tidak pernah melakukan throttling
apa pun. Membuat signature-nya tidak kompatibel mengubah bug produksi yang senyap
jadi compile error.

Alasan yang sama mengatur `NewKeyed`: argumen `Option`-nya dihapus karena cuma
dibuang tanpa dipakai. Clock tetap dipasok di dalam factory, dan memakainya
sebagai `Option` sekarang jadi compile error, bukan no-op yang senyap.

### Penantian tidak pernah menghabiskan kapasitas yang tidak jadi dipakai

`Wait` pada context yang sudah dibatalkan mengembalikan `ctx.Err()` **tanpa
menghabiskan token**. Ini bug fail-open yang ketemu saat proses verifikasi:
loop-nya cuma memeriksa `ctx.Err()` di jalur sleep, sehingga iterasi pertama akan
menghabiskan satu token lalu melaporkan sukses pada context yang sudah mati.
Kedua implementasi `Wait` sekarang memeriksa di awal setiap iterasi, dan sekali
lagi setelah memberikan token.

`Multi` sempat punya lubang yang sama dari arah lain: ia menunggu anak-anaknya
satu per satu, jadi anak pertama sudah membayar sebelum anak kedua sempat
menolak. Ketika anak kedua yang menahan, `Wait` melaporkan gagal sementara token
anak pertama sudah melayang. Sekarang `Multi.Wait` memakai loop yang sama:
me-reserve dulu, menghabiskan hanya setelah **semua** anak setuju.

`Reserve` juga tidak pernah melaporkan nol saat `Allow` menolak. Kontraknya
adalah "nol berarti sekarang", dan konversi token yang kurang jadi durasi dulu
memotong ke arah nol — pada rate tinggi penantian yang nyata membulat jadi `0s`
padahal `Allow` tetap menolak, dan pemanggil yang percaya kontraknya berakhir
busy-spin. Sekarang `Reserve` memberi lantai satu nanodetik selama bucket masih
kurang dari satu token.

### Worker pool memisahkan context-nya jadi dua

`Pool.Start(ctx)` menurunkan context *dispatch* (bisa dibatalkan, menggerbangi
`Dequeue` dan penantian limiter) tapi meneruskan `ctx` asli milik pemanggil ke
handler. `Shutdown` yang graceful cuma membatalkan context dispatch, sehingga job
yang sedang berjalan selesai sampai tuntas alih-alih dibatalkan di tengah handler.

Karena context handler adalah `ctx` itu sendiri, pemanggil tidak boleh menyerahkan
context yang mati saat `Shutdown` berjalan. `cmd/dispatchd` menurunkan context
handler dari context `SIGTERM` lewat `context.WithoutCancel`: nilainya tetap
terbawa, tapi pembatalan sinyal tidak merambat ke job yang sedang melayang.

`Shutdown(ctx)` lalu melakukan drain: berhenti dequeue, menunggu job yang masih
melayang sampai `DrainTimeout`, dan saat kedaluwarsa mengembalikan error yang
membungkus `context.DeadlineExceeded` beserta jumlah job yang masih melayang. Ia
idempotent — panggil dua kali aman, dan panggilan kedua langsung kembali.

### `worker.New` panic pada sink atau handler yang nil

Handler nil dulunya diam-diam diganti no-op, yang berarti setiap job "berhasil"
sambil tidak melakukan apa pun: kehilangan data total yang senyap. Kesalahan
pemrograman pada waktu konstruksi seharusnya berisik. `New` panic dengan pesan
yang eksplisit; `Start(nil)` mengembalikan `ErrNilContext` alih-alih panic di
dalam `context.WithCancel`.

### Error adalah nilai, dan ia dibungkus

Sentinel (`queue.ErrClosed`, `worker.ErrAlreadyStarted`, `eventbus.ErrBusClosed`,
...) diperiksa dengan `errors.Is`, dan error yang dibungkus memakai `%w`. Handler
yang panic dilaporkan sebagai `ErrHandlerPanic` yang dibungkus bersama job ID,
sehingga handler yang bermasalah bisa dibedakan dari job yang gagal secara sah di
hilir.

### Interface didefinisikan di tempat ia dikonsumsi

`worker.Sink` adalah subset empat method dari `*queue.Queue` yang benar-benar
dibutuhkan pool: `Dequeue`, `Ack`, `Nack`, `Extend`. `cmd/dispatchd` meneruskan
queue asli; test meneruskan fake yang bisa diskenariokan. Inilah peribahasa Go
yang diwujudkan — consumer-lah yang mendeklarasikan interface, sehingga pool bisa
di-test tanpa queue sama sekali.

### Pool menjalankan sekumpulan goroutine yang eksplisit

`Start` meluncurkan sejumlah goroutine worker yang tetap (`WithWorkers`) plus
sebuah dispatcher yang memanggil `Dequeue` dan menyerahkan setiap delivery ke
worker yang kosong lewat buffered channel. Worker tidak pernah menyentuh
reservation secara langsung — mereka melaporkan hasilnya, dan jalur pelepasan
yang melakukan ack atau nack. Menjaga jumlah goroutine yang memanggil ke dalam
queue tetap kecil dan berbatas itulah yang membuat locking di queue-nya sendiri
prediktabel.

`InFlight()` dan `Stats()` dilayani dari atomic dan panjang channel, jadi
mengamati pool yang sedang berjalan tidak pernah memblokirnya.

## Benchmark

Diukur, bukan diperkirakan. Semua angka di bawah dari
`go test -bench . -benchtime 100ms` pada **Apple M1 (8 core), go1.26.0,
darwin/arm64**. Reproduksi dengan:

```bash
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
pengiriman (246 ns/op) — dan memang begitulah bentuk `DropNewest` yang
seharusnya; admission control sedang menjalankan tugasnya. Lalu fan-out-nya
linear: ~200 ns per subscriber, jadi biaya broadcast-nya terprediksi, bukan
kuadratik.

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

Semua limiter ini bebas alokasi di jalur panasnya. Penolakan sama murahnya dengan
pemberian, jadi limiter yang sedang menolak trafik bukan beban — dan itu penting,
karena limiter biasanya berjalan pada momen paling diperebutkan justru ketika ia
sedang melakukan throttling. `KeyedGet` di 17.7 ns itu map hit biasa;
`KeyedEviction` membayar 168 B untuk limiter baru, dan itulah biaya jujur dari
memori yang berbatas.

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
channel) yang jadi bottleneck, bukan pool-nya — yang terukur adalah overhead
scheduling plus serah-terima channel, jadi ini lantai, bukan langit-langit.
Handler no-op tidak memakan biaya apa pun, dan itulah sinyal yang dimaksudkan.
`PoolStartShutdown` di 4.7 µs / 25 alokasi memberi tahu kamu bahwa pool berumur
pendek cukup murah untuk dibuat per-test atau per-batch request.

## Apa yang dicakup test

```
$ go test -race -count=1 ./...
ok  github.com/Inc-cryp/go-dispatch/cmd/dispatchd
ok  github.com/Inc-cryp/go-dispatch/eventbus
ok  github.com/Inc-cryp/go-dispatch/queue
ok  github.com/Inc-cryp/go-dispatch/ratelimit
ok  github.com/Inc-cryp/go-dispatch/worker
```

Setiap package jalan dengan `-race`. Di luar test fitur, setiap package punya
`leak_test.go` yang mengambil snapshot `runtime.NumGoroutine()`, mengocok sumber
daya package itu (queue close, siklus subscribe/close bus, start/shutdown pool,
key churn limiter), lalu menegaskan jumlahnya kembali ke baseline — mengulang
hingga 5 detik dan mencetak seluruh `runtime.Stack` saat gagal. Goroutine bocor
adalah mode kegagalan yang baru muncul di produksi berbulan-bulan kemudian, jadi
ia dapat test eksplisit alih-alih sekadar komentar.

`.github/workflows/ci.yml` menjalankan suite yang sama dengan `-race`, ditambah
`make lint` (`fmt-check`, `vet`, `staticcheck`, `golangci-lint`) dan `make smoke`
— yang membangun `dispatchd`, menjalankannya, lalu memeriksa `/healthz` dan
`/stats` benar-benar menjawab. Pipeline-nya tidak mengimplementasi ulang target
apa pun di YAML; ia memanggil `make`, sehingga CI dan mesin lokalmu tidak bisa
berbeda pendapat soal arti "lulus". Seluruh gate itu bisa kamu reproduksi dengan
dua perintah: `make check` dan `make smoke`.

Beberapa perilaku yang dikunci test:

- `TestWaitWithAlreadyCancelledContextDoesNotConsume` — regresi fail-open tadi.
- `TestKeyedIsNotALimiter` — pilihan type-system di atas.
- `TestServeDrainsInFlightHandlersOnSignal` — `serve` menyerahkan context ke
  `Start`, bukan ke `Shutdown`. Selama wiring-nya salah, `SIGTERM` membatalkan
  handler yang justru sedang ditunggu drain, dan job yang melayang kembali
  sebagai "handler interrupted by shutdown".
- `TestDequeueErrorDoesNotRunTheHandlerOnAPhantomJob` — `Dequeue` yang gagal tidak
  pernah menghasilkan delivery, jadi handler tidak boleh dipanggil dengan
  `Delivery` bernilai nol.
- Urutan prioritas versus FIFO untuk prioritas yang sama, termasuk job tertunda.
- Reservation yang kedaluwarsa tidak memakan satu attempt.
- `TestMaxLapsesDeadLettersTheJob` — lapse yang mencapai `MaxLapses` menjadi
  dead-letter dengan `ErrMaxLapses`, sementara `TestMaxLapsesUnsetKeepsTheJobAlive`
  menegaskan `MaxLapses: 0` tetap membiarkan job itu hidup.
- `TestReEnqueueOfALapseDeadLetteredIDForgetsTheOldRecord` — ID yang di-`Enqueue`
  ulang tidak mewarisi catatan lapse dari kehidupannya yang sebelumnya.
- `Nack` mengembalikan `ErrRetryScheduled` saat akan retry, `ErrJobFailed` saat
  dead-letter.
- `Shutdown` pada pool yang sudah berhenti, dan `Close` yang dipanggil dua kali
  pada setiap tipe.
- Handler yang panic menjadi `ErrHandlerPanic` alih-alih membunuh pool.
- `Publish` di bawah policy `Block` terbuka saat bus ditutup.

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

README ini tur singkatnya; [`docs/DESIGN.md`](docs/DESIGN.md) bagian dalamnya —
invariant, taksonomi kegagalan, bug yang ditangkap test, dan apa yang harus
diubah untuk backend yang durable.

Totalnya ~7.800 baris Go termasuk test, di 21 file.

### Flag `dispatchd`

| Flag | Default | Arti |
| --- | --- | --- |
| `-workers` | `8` | Jumlah job runner konkuren |
| `-rate` | `200` | Maksimum job yang dimulai per detik (`0` menonaktifkan limiter) |
| `-burst` | `50` | Kapasitas burst limiter |
| `-addr` | `:8080` | Alamat HTTP untuk listen |
| `-subjects` | `2000` | Jumlah job yang digenerate saat start |
| `-max-lapses` | `0` | Dead-letter job setelah sekian reservation kedaluwarsa (`0` menonaktifkan bound) |
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

Urutan shutdown-nya disengaja: hentikan listener HTTP, lalu drain pool supaya
tidak ada job yang terpotong di tengah jalan, lalu tutup bus **terakhir** supaya
transisi status terakhir tetap terpublikasikan.

## Roadmap

Interface-nya adalah bagian yang menarik; inilah backend-backend yang jadi alasan
bentuknya seperti sekarang.

- [ ] **`queue.Backend`**: ekstrak penyimpanan ke balik sebuah interface, lalu
      tambahkan implementasi Redis (delayed set + reservation berbasis lock) dan
      Postgres (`FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY`).
- [ ] **Idempotency key** supaya consumer at-least-once bisa mendeduplikasi retry.
- [ ] **Fair scheduling**: queue per-tenant dengan weighted round-robin alih-alih
      satu priority heap global.
- [ ] **Adaptive visibility timeout**: perpanjang reservation otomatis saat
      handler melaporkan kemajuan, alih-alih bergantung pada `Extend` yang tetap.
- [ ] **Structured tracing**: propagasikan `traceparent` lewat metadata
      `Entry.Payload` supaya retry sebuah job muncul sebagai satu trace.
- [ ] **Prometheus metrics exporter** untuk counter yang sudah diekspos `Stats()`.

## Lisensi

MIT — lihat [`LICENSE`](LICENSE). Singkatnya: pakai, ubah, dan jual sesuka kamu,
selama notice hak cipta dan izinnya ikut disertakan. Perangkat lunaknya datang
tanpa jaminan apa pun.
