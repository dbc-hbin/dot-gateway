# VM 초기화 후 소스·토큰 파일·게이트웨이·답장 처리 복구

공개 저장소에는 실행 코드와 안전한 예시만 있습니다. 실제 토큰, 토큰 해시,
저장소 파일 식별자, 실제 Discord 대상, 원문 대화와 운영 상태는 없습니다.
이 절차는 깨어 있는 운영자/assistant와 지원되는 실행 도구가 필요합니다.
OS 부팅 자동 실행, VM 소실 후 자기 깨우기, 모델 추론 자동 재개를 보장하지 않습니다.

## 1. 신뢰된 소스와 서로 맞는 비공개 백업

`README.ko.md`의 소스 검증, restore-source, restore-state, restore-operations,
configure-source, 빌드 절차를 먼저 따릅니다. 정확한 Git commit, 외부에 별도로
기록한 SOURCE_MANIFEST SHA, snapshot SHA, operations SHA의 연결을 확인합니다.
빈 DB를 만들어 과거 이벤트를 재처리하거나 더 최신 상태 위에 백업을 덮어쓰지 않습니다.

새 공개 커밋은 새 manifest를 만듭니다. 이전 snapshot에 기록된 manifest를
새 값으로 수동 변경하지 마세요. 이전 백업은 그 백업과 짝인 고정 소스부터
새 private 디렉터리에 복원합니다. 필요한 코드 수정은 검토한 source overlay와
파일별 변경 전/후 hash로 따로 적용합니다. 이 저장소의 일반 overlay 도구는
명시한 새 private tree에만 사용하며 실제 경로/ID 치환을 자동 추측하지 않습니다.

```sh
python3 host-support/apply_source_overlay.py \
  --source "$PRIVATE_SOURCE" --overlay "$PRIVATE_OVERLAY" \
  --manifest-sha "$TRUSTED_OVERLAY_MANIFEST_SHA"
# 검증한 동일 명령에 --apply를 붙여 적용
```

OVERLAY_MANIFEST.json은 schema=1, base_source_manifest_sha256, files만 가집니다.
각 file은 path, sha256(수정 후), configured_base_sha256(현재 private 파일, 새 파일은 null)을
명시합니다. 파일은 private 디렉터리의 0600 자료여야 하며 새 하위 폴더는 먼저
운영자가 준비합니다. 적용은 모든 파일을 검증한 뒤 시작하고 재실행 시 같은 내용은
건너뜁니다. 중간 실패 시 서비스를 시작하지 말고 같은 overlay로 검증/재적용합니다.
UPGRADE_APPLIED.json은 변경 근거이며 활성화 승인이 아닙니다. overlay의 schema는
별도 기존 백업 도구의 schema와 자동 호환되지 않습니다. 기존 전용 overlay는 그
백업에 포함된 검증된 도구로 처리합니다. 둘 중 어느 경로든 재빌드·테스트와 새
바이너리 해시 검증이 필요합니다. 이 저장소가 이전 private overlay 자체를 포함하지는 않습니다.

## 2. 승인된 기존 토큰 파일을 불투명하게 복원

토큰이 GitHub 소스에서 재생성되는 것은 아닙니다. 사용자가 별도로 보관한 기존
파일을 현재 실행기로 안전하게 가져와야 합니다. 이전 실행기의 경로나 만료된 다운로드
URL을 재사용하지 않습니다. 인증정보 전송·설치·영구 접근에 필요한 현재 승인과
직접 입력 요구사항은 그대로 지킵니다. 아래 --apply는 승인을 대신하지 않습니다.

사용자가 승인한 보안 경로로 마련한 입력 파일은 소유자 전용 0700 폴더/0600 일반
파일이어야 합니다. `$STATE_ROOT/secrets`도 먼저 0700으로 준비합니다. 원문을
cat, 셸 변수, 명령 인수, 환경변수 값, 로그, 채팅, 새 공개 백업에 넣지 않습니다.

```sh
python3 host-support/install_token_file.py \
  --input-file "$PRIVATE_TOKEN_INPUT" \
  --destination-file "$STATE_ROOT/secrets/bot-token"
# 필요한 그 시점의 승인을 받은 뒤 운영자가 동일 명령에 --apply를 붙임
```

기본 검증은 토큰을 열지 않습니다. --apply는 길이가 제한된 byte를 그대로 복사하고
해시·내용을 출력하지 않습니다. symlink, hardlink, 공개 권한, FIFO, 빈 파일, 과대 파일을
거부하며 기존 목적지는 덮어쓰지 않습니다. 이미 있는 파일을 교체할 필요가 있으면
멈추고 별도의 승인된 자격증명 교체 절차를 사용합니다. 토큰 유효성은 이 복사 도구가
판단하지 않습니다. 공식 Discord의 읽기 전용 조회로 봇/서버/소유자/채널 권한과
최신 이력을 현재 승인 범위에서 재검증해야 합니다.

## 3. 네이티브 연결 경로와 수신 전용 부트스트랩

도구 호출마다 수명이 끝나는 localhost HTTP(S)_PROXY를 detached 프로세스의
proxy.json에 저장하면 호출 종료 뒤 연결이 실패할 수 있습니다. 해당 VM에서
승인되고 확인된 네이티브 host-lifetime 프록시를 사용합니다. 주소를 추측하거나
다른 VM의 주소를 복사하지 않습니다. `proxy.json`은 기존 schema의
BRIDGE_HTTPS_PROXY/BRIDGE_NO_PROXY만 가지며 인증값 없는 HTTP URL이어야 합니다.
새 launcher는 proxy.json을 자동 덮어쓰지 않습니다. HTTP_PROXY와 다른 상속 설정도
activation-env의 검토된 값으로 치환합니다.

이전 gateway/consumer가 실제로 멈췄는지 해당 executor에서 확인합니다. 다른
프로세스 namespace만 보거나 접근 불가능한 폐기 VM의 종료를 추측하지 않습니다.
불확실한 살아 있는 dispatcher lock이 있으면 새 gateway를 시작하지 않습니다.

새 VM에서 identity·최신 전송 이력·독점 실행을 실제로 확인한 뒤
BOOTSTRAP_RECEIVE_ONLY.json을 예시에 맞게 작성합니다. consumer_verified는 false를
유지합니다. 이 마커는 일반 ACTIVATION.json과 별개이며 RECOVERY_BLOCK.json을
해제하지 않습니다. snapshot/manifest/바이너리/operation 및 과거 blocked 상태가
모두 검증돼야 통과합니다.

지원되는 네이티브 터미널에서 다음을 실행합니다. 짧은 exec 수명이 프로세스를
종료시키는 환경에서는 nohup/setsid만으로 내구성이 보장되지 않습니다.

```sh
sh host-support/recover_gateway_native.sh \
  --source "$PRIVATE_SOURCE" --state-root "$STATE_ROOT" \
  --mode receive-only --wait-connected 25 start
python3 host-support/recover_gateway.py \
  --source "$PRIVATE_SOURCE" --state-root "$STATE_ROOT" status
```

receive-only는 Discord GET과 Gateway 연결만 허용하고 POST/PATCH/PUT/DELETE,
reply sender, typing, feedback, diagnostics, interaction 쓰기를 차단합니다.
catchup은 disarmed_restore를 유지합니다. 이전 대화를 자동 수집/재전송하지 않습니다.
connected_fresh, tracked_process_alive, dispatcher_lock_held를 함께 확인합니다.
실제 새 heartbeat가 생기기 전에는 이전 heartbeat만으로 준비됐다고 표시하지 않습니다.

## 4. 정확히 한 개의 답장 소비자 연결

gateway connected는 assistant가 추론하거나 결과를 받았다는 뜻이 아닙니다.
소비자 실행 도구를 별도로 연결하고 credential-free CLI 결과를 실제로 받을 수
있는지 확인합니다. 진행 중 모델 추론/claim을 백업에서 되살리지 않습니다.

1. 한 logical consumer만 배정하고 stable consumer_id를 고정합니다. 이름이 같아도
   서로 다른 두 controller에서 동시에 polling하면 중복 처리 위험이 있으므로 금지합니다
2. CONSUMER_READY.json에 현재 boot_id와 검증한 controller/exclusivity를 기록하고
   paused=true로 둡니다. 예시는 승인이나 검증 증거가 아닙니다
3. 최신 원격 이력, ledger, 정확한 대상, 바이너리, 소비자 경로를 확인한 후에만
   그 시점의 활성화 권한으로 ACTIVATION.json을 작성합니다. 소비자를 실제 검증한
   경우에만 assistant_consumer_verified=true를 기록합니다
4. gateway에 대한 RECOVERY_BLOCK만 검토 후 해제합니다. 보고서 발행 차단은
   별개입니다. token/proxy 설정이나 marker를 지워 안전 장치를 우회하지 않습니다
5. 아래 stop은 PID, executable, boot ID, start ticks를 대조한 뒤 pidfd로 확인된
   프로세스 하나에만 SIGINT를 보냅니다. pkill이나 이름 기반 일괄 종료는 사용하지 않습니다

```sh
python3 host-support/recover_gateway.py \
  --source "$PRIVATE_SOURCE" --state-root "$STATE_ROOT" stop
sh host-support/recover_gateway_native.sh \
  --source "$PRIVATE_SOURCE" --state-root "$STATE_ROOT" \
  --mode active --wait-connected 25 start
```

새 active heartbeat, receive_only=false, disarmed_restore와 단일 lock을 확인한 뒤
해당 CONSUMER_READY.json의 paused를 false로 바꾸고 그 소비자만 polling합니다.

```sh
python3 host-support/recover_gateway.py \
  --source "$PRIVATE_SOURCE" --state-root "$STATE_ROOT" \
  consumer -- next --wait 30 --lease-seconds 300
```

결과의 현재 claim을 사용해 begin/renew/reply/ignore/delivery를 기존 프로토콜대로
처리합니다. claim 값이나 message 본문은 private 실행 경로 안에 유지합니다.
reply 본문은 private text-file/manifest-file을 사용합니다. 이 adapter는 환경의
token 및 proxy를 제거하고 단일 consumer-command lock을 잡습니다. token을 읽지
않는 Go CLI만 실행합니다. polling 결과가 유실되면 동일 consumer_id로 next를
재개하며 새 이름으로 기존 claim을 피해 가지 않습니다. reply 결과가 유실되면
delivery와 durable state를 확인하고 전송을 무조건 반복하지 않습니다.
status/catchup-status/delivery/cancel-reply/renew는 검증된 같은 소비자에게
일시적 연결 끊김이나 paused 상태에서도 허용됩니다. 새로운 next/reply는 신선한
active 연결을 요구합니다. consumer-command lock은 자식 CLI에도 상속되어
Python wrapper만 종료되었을 때 남은 poll과 새 poll이 겹치지 않습니다.

현재 wrapper는 지원되는 기존 controller를 대신하거나 새 모델을 실행하지 않습니다.
end_to_end_ready는 계속 false이며 신선한 CLI 관찰도 모델 추론 증거는 아닙니다.
소비자가 끊기면 운영 controller가 이 절차로 재연결해야 합니다.

## 5. 보고서·스케줄·한 번만 알림 보존

원본 영수증을 덮어쓰지 않고, snapshot 이후 실제 전송을 읽기 전용 이력과 대조합니다.
원본 nonce/본문 파일 해시가 없으면 꾸며 넣지 않습니다. 별도 검증한 복구 fence는
RECOVERED_DELIVERY_FENCES.json과 reports/recovery-no-replay.json에 동일하게 둡니다.
보고서 guard의 schema는 internal/reporting/recovery.go에 있으며 synthetic 테스트는
필수 필드와 사례를 보여줍니다. 모르는 항목은 비우거나 허용 상태로 추측하지 않습니다.

configure-source로 빌드한 publisher는 복구 gateway root를 고정하므로 임의
--state-dir로 누락된 fence를 피할 수 없습니다. 두 fence의 일치, 승인 플래그,
완료 run, 본문/chunk hash와 정규화 URL을 검사하고 POST 직전 다시 검사합니다.
자료가 누락되면 발행은 차단됩니다. 새 혜택 검토가 필요할 때도 과거 완료 run이나
전송 URL을 바꾸어 차단을 피하지 않습니다.

restore-state만으로 snapshot 이후 fence가 자동 재구성되지는 않습니다. 최신 증거가
없으면 publisher는 계속 차단한 채 gateway 답장 경로만 별도로 복구할 수 있습니다.
기존 스케줄을 조회하고 중복 등록하지 않습니다. 최초 완료 알림 기록은 보존하며
이미 확인한 알림/테스트 메시지를 복구 smoke test로 다시 보내지 않습니다.

## 검증과 한계

Python 3.12+의 Linux pidfd 지원이 host controller에 필요합니다. Go 핵심과 기본
offline 복구는 기존 도구체인을 유지합니다. 이 업데이트는 임시 fixture만 사용하여
권한·링크·no-replace token install, PID 재사용 거부, unknown lock, idempotent start,
단일 consumer gate, receive-only write 차단, catchup 보존과 no-replay를 검사합니다.
실제 토큰/운영 DB/활성 gateway를 테스트에서 사용하지 않습니다. VM 강제 초기화,
OS 부팅, 모델 자기 실행, 외부 Discord 메시지 발송은 검증 범위가 아닙니다.
