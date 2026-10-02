# VM 손실 복구 도구

소스 복원 이후 기존 토큰 파일 설치, 수신 전용 부트스트랩, 네이티브 프로세스 재가동과
단일 답장 소비자 연결은 [VM_RESET_RUNBOOK.ko.md](VM_RESET_RUNBOOK.ko.md)를 함께 따릅니다.
이 문서의 dot-recovery는 계속 offline Go 도구이며, 추가 host controller는 Linux
Python 3.12+ 도구입니다. 기존 snapshot은 그 snapshot에 연결된 소스 manifest와만
직접 호환됩니다. 새 공개 commit으로 hash를 임의 교체하지 않습니다.

## 무엇을 복구하는가

공개 GitHub 소스 + 별도 비공개 운영 메타데이터로 새로운 디렉터리에 기능을 재구성합니다. `dot-recovery` 자체는 Go 프로그램이며 네트워크 전송, 서비스 시작, 패키지 설치, 비밀 복사, 스케줄 등록을 하지 않습니다. 모든 쓰기는 `--apply`가 있을 때만, 기존 경로를 덮어쓰지 않는 새 대상에 수행됩니다.

복구 가능한 것:
- 검토한 소스의 파일별 SHA-256, Go 의존성, Node/Playwright lockfile
- 승인된 봇/서버/소유자/채널 ID, 수신 정책
- 과거 이벤트의 중복 방지 ID, 전송 영수증 ID·해시·nonce·상태
- Go 빌드 명령, 실제 화면을 사용하는 headed Playwright 설정, 미래의 호환 호스트용 서비스 예시

이 백업에 포함하지 않는 것:
- Discord/GitHub 토큰, API 키, 비밀번호, 로그인 쿠키 및 브라우저 프로필
- 원문 대화·답변·수집 본문, 진행 중인 모델 추론, 소비자 세션/claim
- 기존 클라우드 스케줄의 재등록, 모델 소비자의 자동 호출 권한

원문 대화 및 인증정보까지 완전히 되돌리는 백업은 아닙니다. 대화 내용을 제거한 복구 DB는 과거 항목을 읽기 전용 성격의 중복 방지 표식으로 남깁니다. 새로운 정상 메시지는 별도 활성화 검증 뒤에 처리할 수 있습니다.

## 현재 VM의 한계

2026-10-01 점검에서 현재 VM은 PID 1이 `tail`이고 systemd/사용자 systemd가 offline이며 사용할 cron·부팅 서비스가 없었습니다. 파일시스템은 별도 내구성 보장 볼륨으로 검증되지 않은 overlay(`fsync=volatile`)였습니다. 프로세스 supervisor가 살아 있는 동안의 재시작과 VM 재부팅/교체 후 복구는 다릅니다. 이 예시를 저장했다고 현재 VM의 부팅 자동 실행이 생기지 않습니다. `/workspace/shared`도 VM 소실을 견디는 외부 백업이라고 간주하면 안 됩니다.

공식 설명의 “사용 사이에 상태 유지”는 VM 삭제/교체 내구성을 보장하지 않습니다: https://learn.chatgpt.com/docs/dots/computers-and-apps

## 1. 외부에 보관할 복구 세트

1. 공개 소스 커밋의 정확한 Git SHA, SOURCE_MANIFEST.json의 SHA-256
2. `pack-source`로 만든 소스 ZIP과 그 SHA-256 (GitHub 또는 Library의 별도 사본)
3. 비공개 `snapshot.json`과 그 SHA-256. 공개 GitHub에는 절대 추가하지 않습니다
4. 기존 자동화의 위치/ID, 일정/시간대 등 비밀 없는 운영 인수인계 문서. 등록 여부를 먼저 확인하며 중복 등록하지 않습니다
5. 인증정보는 사용자가 승인된 보안 수단으로 별도 관리합니다. 이 도구는 인증정보를 읽거나 보관하지 않습니다

신뢰할 SHA 값을 아카이브 내부에서만 얻으면 변조 탐지가 되지 않습니다. 백업 저장소와 별도로 기록한 승인된 커밋/해시를 사용합니다. 로컬 보관만으로 재해 복구가 완료되지는 않습니다.

이 도구는 검토된 공개 manifest에 나열된 소스만 묶습니다. 일반 폴더 전체를 비밀 탐지 없이 백업하는 도구가 아닙니다. manifest 작성 전 공개 가능 여부를 검토해야 합니다. 추가 파일, symlink, hardlink, 경로 이동, 중복 ZIP 항목, 해시 불일치는 거부합니다. 소스 디렉터리의 manifest 미기재 파일은 아예 읽거나 포함하지 않습니다.

## 2. 도구 준비와 비공개 스냅샷 생성

공식 배포처의 체크섬으로 검증한 Go 1.27.1/linux-amd64를 사용합니다. 명령의 경로는 복구한 위치에 맞게 지정합니다. 현재 기록된 버전은 TOOLCHAIN.lock.json에 있습니다.

```sh
umask 077
mkdir -p "$HOME/dot-recovery-safe"
chmod 700 "$HOME/dot-recovery-safe"
export GO_BIN=/absolute/path/to/go1.27.1/bin/go
export GOTOOLCHAIN=local
cd /absolute/path/to/reviewed-source/recovery
"$GO_BIN" test -mod=readonly -race ./...
"$GO_BIN" vet -mod=readonly ./...
CGO_ENABLED=0 "$GO_BIN" build -mod=readonly -trimpath -o "$HOME/dot-recovery-safe/dot-recovery" .
```

운영 설정은 templates/operation.example.json의 명시된 ID·정책 필드만 실제 승인된 값으로 채워 0700 폴더 안에 0600 파일로 둡니다. 비밀을 넣지 않습니다. 모르는 필드는 허용되지 않습니다. 아래 명령은 우선 쓰지 않는 검증 실행이며, 성공 후 같은 명령에 `--apply`를 붙이면 새 파일을 생성합니다.

```sh
dot-recovery snapshot \
  --db /absolute/live-private/bridge.sqlite3 \
  --reports /absolute/report-state/outbox \
  --operation /absolute/private/operation.json \
  --manifest-sha "$TRUSTED_MANIFEST_SHA" \
  --output /absolute/private/snapshot.json
```

DB는 읽기 전용 SQLite 트랜잭션으로 선택한 메타데이터 컬럼만 읽습니다. 본 DB만 복사하지 않으므로 WAL 데이터의 불완전 복사 문제를 피합니다. 원문 envelope/text/content, runtime 값, claim, provider key는 조회하지 않습니다. DB/sidecar는 소유자·개인 권한·일반 파일·단일 링크 조건을 검사합니다. 부모 디렉터리 FD를 유지해 경로를 고정합니다. 같은 UID 전체가 이미 침해된 환경에 대한 보안 경계는 아닙니다.

보고서 경로는 target.json과 run-*.json이 직접 들어 있는 outbox 디렉터리여야 합니다. target.json의 승인 대상과 운영 설정이 다르면 중단합니다.

동시에 실행되는 발행기의 run 파일은 원자적으로 교체되는 단위로 읽습니다. 스냅샷 직후의 새 전송은 이 스냅샷에 없습니다. 따라서 마지막 스냅샷 이후 전송 내역을 확인하기 전에는 복구 서비스를 활성화하지 않습니다. 운영 중 반복 백업·외부 업로드는 별도로 승인된 실행 경로가 필요합니다.

## 3. 소스 복원

공개 저장소는 GitHub 인증정보 없이 읽을 수 있습니다. 신뢰할 커밋을 직접 지정하고 새 checkout을 사용합니다. GitHub가 불가능하면 검증된 Library 소스 ZIP을 다운로드하고 SHA를 비교합니다.

```sh
git clone https://github.com/dbc-hbin/dot-gateway.git source-checkout
git -C source-checkout checkout --detach "$TRUSTED_COMMIT"
dot-recovery verify-source --source "$PWD/source-checkout" --manifest-sha "$TRUSTED_MANIFEST_SHA"
dot-recovery pack-source --source "$PWD/source-checkout" --manifest-sha "$TRUSTED_MANIFEST_SHA" --output "$HOME/dot-recovery-safe/source.zip" --apply

dot-recovery restore-source --archive "$HOME/dot-recovery-safe/source.zip" \
  --manifest-sha "$TRUSTED_MANIFEST_SHA" \
  --new-root "$HOME/dot-recovery-safe/restored-source" --apply
```

새 root의 부모가 실제 사용자 소유의 0700 디렉터리여야 합니다. 최종 이름이 이미 있으면 빈 디렉터리여도 중단합니다. 숨겨진 `.recovery-stage-*`에 모두 기록하고 파일/디렉터리를 fsync한 다음 Linux renameat2(RENAME_NOREPLACE)로 설치합니다. 중단된 stage는 실행하지 말고 조사 후 정리합니다. 다른 파일시스템으로 이동하거나 덮어쓰는 fallback은 없습니다. 실제 전원 손실 내구성은 호스트 스토리지의 fsync 보장에 달려 있습니다.

## 4. 비공개 상태 복원과 승인된 설정 적용

```sh
dot-recovery verify-state --snapshot "$PRIVATE_SNAPSHOT" \
  --snapshot-sha "$TRUSTED_SNAPSHOT_SHA" --manifest-sha "$TRUSTED_MANIFEST_SHA"
dot-recovery restore-state --snapshot "$PRIVATE_SNAPSHOT" \
  --snapshot-sha "$TRUSTED_SNAPSHOT_SHA" --manifest-sha "$TRUSTED_MANIFEST_SHA" \
  --new-root "$HOME/dot-recovery-safe/restored-state" --apply

dot-recovery configure-source --source "$HOME/dot-recovery-safe/restored-source" \
  --manifest-sha "$TRUSTED_MANIFEST_SHA" \
  --snapshot "$PRIVATE_SNAPSHOT" --snapshot-sha "$TRUSTED_SNAPSHOT_SHA" \
  --state-root "$HOME/dot-recovery-safe/restored-state" \
  --new-root "$HOME/dot-recovery-safe/private-build-source" --apply
```

공개 소스의 발행 대상은 의도적으로 예시 ID에 고정돼 있습니다. `configure-source`는 원본 해시와 스냅샷의 연결을 확인한 뒤 별도 비공개 소스에만 승인된 target과 새 token/proxy 경로를 정확히 치환합니다. upstream 패턴이 달라지면 중단합니다. BASE_SOURCE_MANIFEST.json, 새 SOURCE_MANIFEST.json, PRIVATE_DERIVATION.json으로 변경 근거를 남깁니다. 이 비공개 파생 소스·설정은 공개 저장소에 올리지 않습니다.

복원 처리 규칙:
- 모든 이전 inbound 및 미해결 ingress는 inbound의 `blocked` 중복 방지 표식이 됩니다. lease/claim을 복원하지 않습니다
- 이미 sent인 답변 chunk의 receipt ID는 유지합니다. 다른 상태는 전부 `uncertain`이며 자동 전송 큐에 들어가지 않습니다
- 이전 진단 전송/테스트 전송도 `uncertain`, attempted=1 표식으로 복원해 진단 명령 재실행이 과거 포스트를 다시 보내지 않게 합니다
- 완료된 보고서 영수증은 필드·해시·nonce·완료 상태를 그대로 보존합니다. 미완료 chunk는 전부 `uncertain`으로 바꾸고 원래 전송 시각 또는 스냅샷 시각을 남깁니다
- 발행기가 같은 run ID와 같은 본문을 다시 받으면 완료 영수증을 그대로 반환합니다. uncertain+message ID는 GET 재확인을 할 수 있지만 POST는 재시도하지 않습니다. 이 백업에는 본문이 없으므로 본문을 재생성하거나 run ID를 변경해 보내지 않습니다
- 새로운 `RECOVERY_BLOCK.json`, `RECOVERY_STATE.json`(출처·원래 개수), `RESTORED_SNAPSHOT.json`(내용 없는 원본 메타데이터)이 항상 생깁니다. 두 시작 스크립트는 차단 파일 부재만으로 시작하지 않고 양성 활성화 검증을 실행합니다. 기존 임의 바이너리를 직접 실행하면 이 파일을 검사하지 않으므로 수동 실행도 활성화 절차를 따라야 합니다

## 5. 의존성 설치 및 빌드

공식 Go https://go.dev/dl/ 및 Node https://nodejs.org/ 배포처에서 기록된 버전과 체크섬을 확인합니다. Chromium은 신뢰할 공식 OS 패키지 저장소에서 기록된 151.0.7922.173 빌드를 설치하거나, 해당 버전이 더 이상 제공되지 않으면 새 버전을 보안·호환성 검토한 뒤 기록을 갱신합니다. 임의 다운로드 사이트나 인증서 오류 우회는 사용하지 않습니다. 브라우저/OS 라이브러리 및 실제 그래픽 세션 설치는 호스트별 단계이므로 무인 복구로 가정하지 않습니다.

```sh
cd "$HOME/dot-recovery-safe/private-build-source"
export GO_BIN=/absolute/path/to/go1.27.1/bin/go
# 쓰기 가능한 캐시 위치를 필요할 때 지정
export GOPATH="$HOME/dot-recovery-safe/go-path"
export GOCACHE="$HOME/dot-recovery-safe/go-cache"
sh recovery/build.sh deps
sh recovery/build.sh build
sh recovery/build.sh runtime-check
```

Go module/go.sum 및 npm lockfile의 정확한 버전을 사용합니다. `npm ci --ignore-scripts`는 lifecycle script를 실행하지 않습니다. Python은 이 복구 경로의 런타임 필수 조건이 아닙니다. Go gateway/collector/broker + Node/Playwright의 실제 headed Chromium 구조를 유지합니다. headless/Xvfb/`--no-sandbox`로 대체하거나 사용자 브라우저 프로필을 복사하지 않습니다.

## 6. 활성화 전 점검 및 보안 경계

일반 `activation-env --component gateway`는 기존 비공개 DB/schema, 토큰 파일의 안전한 메타데이터, 명시적 프록시를 확인합니다. 수동 ACTIVATION 기록·바이너리 해시 승인·소비자/이력 확인 서류를 요구하지 않으며 RECOVERY_BLOCK을 지우지 않습니다. 봇 신원과 정확한 대상 범위는 게이트웨이가 검증합니다. 과거 blocked 이벤트·전송 영수증·비활성 catch-up은 변경하지 않습니다. 아래의 수동 활성화 및 해시 연결 요구는 `headed`와 별도 `gateway-receive-only` 경로에 적용됩니다.

복구 도구는 비밀을 생성·이동·읽지 않습니다. 사용자가 승인된 보안 절차로 `restored-state/secrets/`(0700)에 bot-token(0600)을 직접 복원해야 합니다. 새로운 OAuth/토큰/영구 접근·부팅 설정은 별도 행위 시 승인이 필요합니다. GitHub 토큰은 소스 읽기와 이 복구 도구 실행에 필요 없습니다. 프록시는 승인된 비밀 없는 설정만 `restored-state/proxy.json`에 둡니다. URL에 사용자명/비밀번호를 넣지 않습니다.

```sh
dot-recovery health \
  --db "$HOME/dot-recovery-safe/restored-state/bridge/bridge.sqlite3" \
  --queue "$HOME/dot-recovery-safe/restored-state/browser-queue" \
  --token-file "$HOME/dot-recovery-safe/restored-state/secrets/bot-token"
```

health는 기계 판독 JSON입니다. credential `missing`, `insecure`, `empty`, `present_metadata_only`를 구분하며 내용은 절대 열지 않습니다. gateway의 신선한 connected heartbeat와 broker의 boot ID/start ticks/PID 일치·headed·sandbox 상태를 확인합니다. 브로커 heartbeat는 실제 웹페이지 캡처 성공을 뜻하지 않으며 별도 승인된 smoke test가 필요합니다. `end_to_end_ready`는 항상 false이며 모델 소비자의 활성/자동 깨어남은 이 도구가 증명할 수 없습니다.

스냅샷 이후 Discord의 실제 영수증·최신 이벤트 및 기존 자동화 등록을 읽기 전용으로 대조합니다. 불명확하면 전송 차단을 유지합니다. 승인된 target, 권한, 프록시, 새 바이너리 해시, 실제 화면, 토큰, 최신 ledger, 소비자 연결을 점검하고 이전 sender가 확실히 중지돼 중복 실행되지 않는 것을 확인한 다음에만 별도 승인으로 활성화를 진행합니다.

활성화 스키마는 `templates/ACTIVATION.example.json`에 있습니다. 이 예시의 기본값은 모두 미승인(false) 또는 placeholder입니다. 승인된 운영자가 실제 값을 검증한 뒤에만 새 상태 root의 `ACTIVATION.json`(0600)에 기록합니다. 필드는 schema=1, snapshot_sha256, source_manifest_sha256(공개 기반 manifest 해시), gateway_binary_sha256, broker_script_sha256, authorized_at/ latest_history_verified_at(RFC3339 UTC), discord_identity_verified, assistant_consumer_verified, previous_sender_stopped입니다. 마지막 세 값은 실제 검증과 별도 승인 없이는 true로 바꾸지 않습니다. 예시 파일을 단순 복사해서는 시작할 수 없습니다.

`sh recovery/build.sh build`가 파생 소스의 `recovery/dot-recovery` 바이너리를 만듭니다. 생성된 launch 스크립트는 바로 그 절대 경로로 `activation-env`를 호출합니다. 이 명령은 서비스 실행 없이 마커의 해시 연결, 원래 이벤트-ID별 blocked 표식, 전송 ledger의 비재생 상태, 기존 private DB, 기존 credential의 메타데이터, 검토된 gateway 바이너리/브라우저 script 해시를 확인합니다. 실패하면 shell은 즉시 중단합니다. 성공 시에만 고정된 프록시 환경변수 export를 반환하고, 상속된 DISCORD_BOT_TOKEN 및 프록시 변수를 먼저 제거합니다. 과거 메시지 본문이나 인증정보는 출력하지 않습니다. 프록시 설정은 실제 세 구성요소 공통 스키마인 BRIDGE_HTTPS_PROXY/BRIDGE_NO_PROXY를 사용합니다.

승인이 끝난 뒤 운영자가 차단 파일을 해제해도 긍정적 ACTIVATION 기록·기존 DB·원래 ID별 ledger가 없으면 시작되지 않습니다. 실제 기록의 마지막 시점 이후 누락된 전송까지 자동으로 알아내는 것은 아니므로 최신 원격 이력 대조가 여전히 필수입니다. 본 작업은 서비스 재시작/재부팅/자동화 중복 등록/외부 POST를 수행하지 않습니다.

`templates/*.service.example`은 systemd가 실제 동작하는 미래 호스트에서만 검토 후 설치할 수 있는 예시입니다. 현재 VM에서 사용할 수 없습니다. desktop 로그인 없는 headed 서비스 자동 실행을 보장하지 않으며, user linger나 네트워크/권한 설정은 자동 적용하지 않습니다. 실제 재부팅 시험 전에는 부팅 복구가 검증됐다고 표시하지 않습니다.

## 검증 범위

`go test -race ./...`와 `go vet ./...`은 별도 임시 디렉터리·합성 DB·임시 sleep 프로세스로만 검사합니다. 테스트는 잘못된 hash/nonce, duplicate ledger/event, archive traversal, symlink/hardlink, 기존 root 보호, 쓰기 중단 rollback, 비공개 권한, 원문 비포함, pending 전송 0건 및 PID 재사용 거부를 포함합니다. 실제 gateway DB migration/중복 이벤트 및 publisher 동일 run 재실행은 독립 통합 검증 결과를 함께 확인합니다. 테스트 통과는 전원 손실·부팅·모델 소비자 자동 가동 성공의 증거가 아닙니다.

## 수집기·첫 알림 상태도 함께 복구하기

이 추가 스냅샷은 본문 없는 메인 snapshot.json과 정확한 소스 manifest에 연결됩니다. 후보 수집 범위를 넓혀도 이전 혜택 전달 기록과 한 번만 하기로 한 ChatGPT 결과 알림을 잊지 않게 보존합니다.

- state.json: 수집기 version=1, URL→SHA-256 fingerprints 및 선택적 wide_reviewed_v1(URL→양의 검토 순번). wide 표식은 실제 fingerprint URL의 부분집합이며 최대 2,500개입니다
- reviewed-offers.json: 상세/간략 검토 항목, 혜택 ID와 원래 URL, 전달 run/payload 해시/정확한 Discord 영수증
- first-auto-notification.json: 이미 수락된 첫 ChatGPT 확인의 message ID와 해당 run의 Discord 영수증. 새로운 알림을 보내지 않습니다
- schedule.json: 기존 스케줄 ID·간격·시간대의 역사적 기록. enabled=true는 새 스케줄 등록 권한이나 등록 완료를 의미하지 않습니다
- RUNBOOK.md: 당시 운영 안내를 그대로 남기는 읽기 전용 자료. 내용이나 과거 경로를 자동 실행하지 않습니다

운영 파일 다섯 개는 단일 트랜잭션으로 갱신되지 않습니다. 이 명령의 두 번 읽기 비교는 수집 중 변경 여부만 확인하며 원자적 다중 파일 일관성을 증명하지 않습니다. 반드시 실제 scan/review/delivery와 상태 commit이 끝나고 새 실행이 없는 시점에 백업합니다.

JSON에 모르는 필드·중복 키가 있으면 무시하지 않고 중단합니다. 알려진 인증정보 패턴도 거부합니다. 외부 원문 scan/report 본문·인증정보·프로파일을 이 명령으로 묶지 않습니다. DCInside는 검증된 ai_utilize 게시물의 id/no query만 보존하며 임의 query는 허용하지 않습니다.

```sh
# 먼저 최신 완료 상태와 outbox로 메인 snapshot을 생성·검증합니다.
dot-recovery snapshot-operations \
  --operations-root /absolute/private/ai-benefits-cron \
  --snapshot "$PRIVATE_SNAPSHOT" --snapshot-sha "$TRUSTED_SNAPSHOT_SHA" \
  --manifest-sha "$TRUSTED_MANIFEST_SHA" \
  --output /absolute/private/operations-snapshot.json
# 검증 성공 후 같은 명령에 --apply를 붙여 새 파일을 생성합니다.

dot-recovery verify-operations \
  --operations "$PRIVATE_OPERATIONS" --operations-sha "$TRUSTED_OPERATIONS_SHA" \
  --snapshot "$PRIVATE_SNAPSHOT" --snapshot-sha "$TRUSTED_SNAPSHOT_SHA" \
  --manifest-sha "$TRUSTED_MANIFEST_SHA"

dot-recovery restore-operations \
  --operations "$PRIVATE_OPERATIONS" --operations-sha "$TRUSTED_OPERATIONS_SHA" \
  --snapshot "$PRIVATE_SNAPSHOT" --snapshot-sha "$TRUSTED_SNAPSHOT_SHA" \
  --manifest-sha "$TRUSTED_MANIFEST_SHA" \
  --new-root "$HOME/dot-recovery-safe/restored-collector" --apply
```

복원된 수집기 root는 state.json, reviewed-offers.json, first-auto-notification.json, schedule.json, RUNBOOK.md와 메인 스냅샷에 연결된 outbox/target.json·run 영수증을 함께 가집니다. 새 빈 outbox를 만들지 않습니다. 완료 영수증은 그대로, 미확정 영수증은 uncertain으로 보존합니다. RECOVERY_BLOCK.json도 항상 생성합니다.

재가동 전에 기존 자동화가 이 새 root를 사용하도록 승인된 환경 설정을 검토해야 합니다. RUNBOOK의 과거 절대 경로는 자동 변경되지 않습니다. 이미 보낸 혜택은 reviewed-offers와 outbox를 모두 확인하고, first-auto-notification의 기존 수락 기록을 지우거나 다시 보내지 않습니다. 이 명령은 스케줄러 API나 ChatGPT/Discord에 접근하지 않습니다. 최신 이력 대조·기존 sender 중지·별도 활성화 승인은 여전히 필요합니다.

## 비공개 대화 메모리 연결

새 gateway의 `CONVERSATION_MEMORY.md`를 함께 따릅니다. 메인 metadata 스냅샷은
대화 본문 없이 메모리 ledger ID, source revision/state/정확한 scope hash,
forget 표식과 fact version만 보존합니다. 실제 redacted 메모리는 별도 0600
`memory-export` 아카이브와 독립적으로 기록한 SHA-256으로 비공개 보관합니다.
둘은 같은 일관된 SQLite 스냅샷에서 생성합니다.

`restore-state`는 메모리 경계값을 inert fence 테이블에만 복원합니다. 별도
`memory-import --sha256`는 기존 source head/claim/전송 작업을 재생성하지 않고,
같은 소유자·대화 scope의 `restored_unverified_history`로만 복원합니다. 더 최신
metadata의 삭제/forget/버전 표식은 이전 메모리 아카이브보다 우선합니다. 복원 후
다시 백업해도 이 표식은 유지됩니다. 양쪽 백업 모두 삭제 이전이라면 나중의 삭제를
알아낼 수 없습니다. 기존 memory metadata가 없는 스냅샷 형식도 계속 지원합니다.

## 등록 명령·컨트롤 이력 복원

선택적 phase3 metadata는 승인된 앱/서버/소유자에 고정된 ask/status/cancel 명령 ID,
등록 시도 표식, 토큰 없는 interaction 중복 방지, reaction 전이, 취소 및 최소 첨부
영수증 정보를 보존합니다. 등록 API를 다시 호출하거나 오래된 요청을 실행하지
않습니다. 첨부 정보는 ID/크기뿐인 별도 inert 표식이며 실제 파일/본문/URL/파일명은
복원하지 않습니다. 모든 복원은 catchup을 disarmed_restore 상태로 유지합니다.
정확한 포함·제외 목록과 재활성화 한계는 PRIVATE_BACKUP_ALLOWLIST.md를 확인합니다.

## Worker-control 복구 경계

수정된 exporter는 worker의 미해결 실행 요청 ID, 복구 대기 요청 ID, 취소 상태와
시각만 별도 비활성 메타데이터로 보존합니다. claim·worker/controller 이름·실행 세대·
증거 본문·lease·원문 prompt는 내보내지 않으며, 복원 후에도 과거 요청은 blocked입니다.
미해결 실행/취소 수치는 state_counts에서 확인합니다. 이는 worker 자동 재시작이나
중단 확인이 아닙니다. 새 네이티브 worker를 시작하기 전에 실제 controller가 기존
실행과 취소를 별도로 확인해야 합니다. VM 복원이나 transport 정상 상태만으로 기존
클라우드 추론이 끝났다고 판단하거나 취소 ACK를 만들면 안 됩니다.
