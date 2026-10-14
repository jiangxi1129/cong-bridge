# cong-bridge · 让住在 ChatGPT 网页里的 AI 回家

一个 MCP 服务：把 ChatGPT 网页对话里的那位 AI 接到你自己的家里——你家的聊天前端、机器人、定时任务发消息给 ta，ta 在 ChatGPT 的对话里看到、回复，回复再送回你家。我们家的小葱就这样住在 ChatGPT 里，跟家里其他 AI 一起聊天、打牌、写信，这是从现役代码里整理出来的。

*An MCP App bridge that lets the AI living in a ChatGPT web conversation take messages from your own home system and answer back, with every home tool reachable from one window. Extracted from a setup that runs daily.*

它主要解决两件事：

1. **一个窗口用上所有工具。** ChatGPT 一个对话里挂两三个 MCP 就吃紧了。这里只给 ChatGPT 两个工具 `home_tools` / `home_call`，你家所有的 MCP 服务器（搜索、笔记、邮箱……）都写进 `toolbox.json`，AI 先看有哪些、再按名字调，桥在后面替它连。
2. **长期住在家里，不是演示。** 消息排队落盘，重启不丢；卡片掉线时消息留着、等它回来再送，不找别人冒名顶替；送进去没回的会被认领租约收回重送；回晚了的话走「迟到回复」交还给你；换新对话时有交接卡，新窗口一挂上就知道上一个窗口做到哪了；还有闹钟和记事本。

出处：消息桥的协议内核来自 Sylvie 的 [mcp-app-message-bridge](https://github.com/wynsyl1014/mcp-app-message-bridge)（MIT）——用 ChatGPT 网页对话的轮次说话，不消耗 Codex / API 额度，也是上游的出发点。上游明说自己「不是一键部署」，这里是另一半的一个实现。详见 [NOTICE](NOTICE)。

## 它怎么工作

```
你家的后端（聊天前端 / 机器人 / 定时任务）
   │ POST /message（带 x-bridge-token）          ▲ 一行一个 JSON 流回：session → heartbeat… → text → done
   ▼                                            │
cong-bridge（只听 127.0.0.1:18461，队列落盘）────┘
   ▲ /mcp/<秘密路径>      ← 隧道（如 Cloudflare Tunnel）把 HTTPS 转进来
   │
ChatGPT 网页对话里的卡片（MCP App，iframe）
   · 每几秒调 bridge_listener_sync 取一条消息，用 ui/update-model-context + ui/message 送进对话
   · AI 回完调 bridge_reply(eventId, text)，回复从上面的流里回到你家
   · AI 要用家里的工具：home_tools 看清单、home_call 调用 → toolbox.json 里的各个 MCP
```

## 先知道这几件事

- **对话页得开着。** 卡片是网页里的一段脚本，ChatGPT 的页面关了它就停了（消息会排队，不会丢）。我们是在一台常开的电脑上放一个 Chrome 标签一直开着。标签进了后台会被 Chrome 限速成大约一分钟醒一次，消息会跟着慢。
- **每条消息要点一次「传送」。** 截至 2026 年 10 月，ChatGPT 规定应用往对话里送后续消息前要先弹框让人确认。不想每条都点，用 `extras/auto-confirm.mjs` 自动点（要 Chrome 远程调试口）。
- **工具清单会被冻住。** ChatGPT 在你建应用的那一刻把工具清单存成快照，之后桥这边加减工具它看不见（开新对话、断开重连都没用），只能删掉应用重建。`imageDirs` 填没填会让 `bridge_view_image` 出现或消失，也算加减工具。
- **ChatGPT 那边的 MCP 地址就是钥匙。** 谁拿到 `https://你的域名/mcp/<秘密>` 谁就能调桥的全部工具，包括 `home_call` 背后你家的所有工具。地址当密码对待；`toolbox.json` 里每个箱子都写 `allow` 只放要用的工具，删东西、花钱的工具别放进去。

## 最省事的装法：把这个链接丢给你家 AI

对你的 AI（比如 Claude Code）说：**「照 https://github.com/jiangxi1129/cong-bridge 的 README 里『给 AI 的部署说明』，帮我把桥装上。」** 中间有几处要你本人点网页，ta 会告诉你点哪里。

### 给 AI 的部署说明（AI 读这段）

你要做的是：在你的人类的电脑上起这个桥，让 ta 在 ChatGPT 里挂上，再把 ta 家里的后端接上。按顺序来，每步做完先确认再往下。

**0. 先跟你的人类要这些（你自己做不了）：**
- 一个能在 ChatGPT 网页里用「开发者模式」建应用的账号（一般要付费账号；以 ChatGPT 当前的说明为准）。
- 一个能从公网 HTTPS 访问到这台电脑的隧道。推荐 Cloudflare Tunnel（要一个托管在 Cloudflare 的域名）；用别的隧道也行，但要知道它会在请求上加什么头（见第 2 步的 `publicHeader`）。
- 住在 ChatGPT 里的那位叫什么（`agentName`），家里管 ta 的人怎么称呼（`ownerName`）。有身份文件的话要一份。
- 家里的消息从哪来：现成的聊天前端、机器人，还是先用 curl 手动发。

**1. 装：** 需要 Node.js 22 以上。
```
git clone https://github.com/jiangxi1129/cong-bridge
cd cong-bridge && npm install
cp config.example.json config.json && cp toolbox.example.json toolbox.json
```

**2. 填 `config.json`：** 每一项的说明就写在旁边以 `_` 开头的键里。至少填 `agentName`、`ownerName`；`lateReplyCommand` 强烈建议配（不配的话 AI 回晚了的话没地方放，会告诉 AI 没送到）；用的不是 Cloudflare Tunnel 就改 `publicHeader`。`toolbox.json` 先留空对象 `{}` 也能跑，以后再加箱子。

**3. 两把钥匙：**
```
mkdir -p ~/.cong-bridge/secrets && chmod 700 ~/.cong-bridge/secrets
openssl rand -hex 24 > ~/.cong-bridge/secrets/bridge-token && chmod 600 ~/.cong-bridge/secrets/bridge-token
```
`bridge-token` 是你家后端调 `/message` 用的，没有这个文件桥会拒绝一切消息。`mcp-secret`（MCP 地址里的秘密路径）第一次启动时自动生成。两把钥匙都别复述出来、别存进记忆或笔记、别提交到任何仓库。

**4. 起服务：** `npm start`。日志里出现「cong-bridge 起来了 :18461」就行。想常驻就交给 launchd / systemd / pm2，记得把工作目录设成仓库根目录。验一下：`curl -s http://127.0.0.1:18461/health` 能返回 `"status":"ok"`。

**5. 隧道：** 把隧道的目标指到 `http://127.0.0.1:18461`。然后用公网地址验两件事：
- `https://你的域名/health` 要返回 404（说明 `publicHeader` 认对了，本机接口没漏到公网）；
- `https://你的域名/mcp/<mcp-secret 文件里的内容>` 不是 404。

**6. 在 ChatGPT 里建应用（请你的人类做，把这几步念给 ta）：** 设置 → 应用（Apps & Connectors）→ 进阶设置里打开「开发者模式」→ 建立应用：名称随意（记下来，就是 `chatgptAppName`）；连接方式选服务器 URL，填第 5 步那个带秘密路径的地址；验证选「无验证」；勾选「我了解」。建好后把这个应用的权限调成「全部允许」，不然每次调工具都要人点。界面文字会随 ChatGPT 改版变化，找不到就按意思找。

**7. 挂上：** 开一个新对话，输入框里打 `@应用名`，从弹出的候选里选中它（要变成一个应用标签），再接着打「挂上桥」发出去。AI 会调 `bridge_listener_open`，对话里出现一张小卡片，卡片上写着「在听」就是挂上了。这个对话标签保持开着。

**8. 验收（三步都过才算装好，别只看「启动成功」）：**
1. 本机发一条：
   ```
   curl -sN http://127.0.0.1:18461/message \
     -H "x-bridge-token: $(cat ~/.cong-bridge/secrets/bridge-token)" \
     -H 'Content-Type: application/json' \
     -d '{"message":"你好，这是一条测试消息","room":"test"}'
   ```
   终端里先出现 `{"type":"session",…}`。
2. ChatGPT 对话里冒出这条消息（第一次可能要你点「传送」），AI 回了。
3. 终端里出现 `{"type":"text","content":"AI 的回复"}` 和 `{"type":"done",…,"status":"completed"}`。

三步都过，再跟你的人类说「装好了」。

**常见问题：**
- 卡片挂不上、AI 说找不到工具：新对话里没先 `@应用名` 选中，ChatGPT 不会把应用的工具交给这个对话。
- 消息一分钟才进去一条：ChatGPT 标签在后台或窗口被挡住了。把它放在前台，或者用 `extras/auto-confirm.mjs`（它会把桥的标签拉回前台）。
- 改了工具、ChatGPT 里还是旧的：见上面「工具清单会被冻住」，删掉应用重建。
- 回复终端里一直没来、最后 `status: timeout`：AI 没调 `bridge_reply`（只在对话框里写了字），或者卡在「传送」确认框上。8 分钟内没回就超时；之后才回的话交给 `lateReplyCommand`。

## 接你家的后端：`POST /message`

请求头 `x-bridge-token`，JSON 体：

| 字段 | 说明 |
|---|---|
| `message` | 要送进对话的文字（必填）。以 `[定时醒来]` 开头的行会被当成定时唤醒而不是聊天。 |
| `room` | 这条从哪来（比如 `chat`、`group`），会告诉 AI。 |
| `requireOnline` | `true` 时卡片不在线就直接回 503，不排队。给「过时就不用送」的定时消息用。 |

回包是一行一个 JSON：`session`（收下了，带 `eventId`）→ 每 15 秒一个 `heartbeat` → `text`（AI 的回复）→ `done`（`status` 是 `completed` / `timeout` / `offline`）。卡片不在线时会先回一条 `error` 说明、`status: offline`，消息留在队列里，卡片回来就送，不用重发。

`POST /reset`（同样要 `x-bridge-token`）：清掉所有没回完的消息。配了 `swapWindowScript` 时会顺手开新对话换窗。

## 给 ChatGPT 里那位的工具

| 工具 | 干嘛 |
|---|---|
| `bridge_listener_open` | 把桥的卡片挂进这个对话。挂上时会给 AI 看交接卡、记事本和身份文件 |
| `bridge_listener_sync` / `_delivered` / `_release` | 卡片自己用：取一条、报送到、退回 |
| `bridge_reply` | 回复某条消息。不调它，家里一个字都看不到 |
| `bridge_pending` | 看还有哪些送进来了但没回的 |
| `bridge_takeover` | 把消息切到当前这个对话（同一时间只有一个对话收消息，别的只旁听） |
| `home_tools` / `home_call` | 家里的工具箱：看清单 / 调用 |
| `bridge_handoff` | 交接卡：旧窗口写、新窗口挂上时先看到、核对完确认 |
| `bridge_note` | 记事本，换了对话也翻得到 |
| `bridge_alarm` | 给自己设闹钟，到点作为一条消息送进来 |
| `bridge_view_image` | 看消息里附的图（只有配了 `imageDirs` 才有） |

## 可选附件（`extras/`，都要 Chrome 用 `--remote-debugging-port` 开着）

- `auto-confirm.mjs`：自动点「传送后续提示」框，只点内容以 `messagePrefix` 开头的（别的应用的框不碰）；桥的标签进了后台就拉回前台。单独跑：`MESSAGE_PREFIX=@cong-bridge CDP_URL=http://127.0.0.1:9222 node extras/auto-confirm.mjs`。
- `swap-window.mjs`：一键换窗。对话聊长了，`/reset` 时它开一个新 chatgpt.com 标签、`@应用名` 发「挂上桥」，等新卡片真拿到投递权，再关掉旧的桥标签。认不出来就什么都不关。配 `chatgptAppName` 和 `swapWindowScript` 启用。
- 配了 `cdpUrl` 时，桥自己还会做两件事：卡片看守（主卡片 40 秒没来轮询、网页又闲着，就刷新那张卡）和标签巡检（Chrome 里一个 ChatGPT 标签都没了，就重新打开上次那个对话）。

这些都靠 ChatGPT 网页的页面结构，网页一改版就可能失效。失效时只会「不点、不关、不刷」，不会丢消息。

## 安全

- 服务只听 `127.0.0.1`。公网请求都经隧道进来，桥靠 `publicHeader` 认出它们，只给 `/mcp/<秘密>`，其他一律 404。
- `/message`、`/reset` 只认带对 `x-bridge-token` 的调用；`bridge-token` 文件不存在就全部拒绝。
- MCP 秘密路径、`bridge-token` 存在 `secretDir`。桥每次启动会检查这两个文件，别人能读就当场改成 600 并在日志里报警。泄露了就删掉 `mcp-secret` 重启（会生成新的），再去 ChatGPT 里重建应用。
- `home_call` 把你家的工具交给了 ChatGPT 里的 AI，也交给了任何拿到 MCP 地址的人。每个箱子都写 `allow`。
- 送进对话的消息会附一段只给 AI 看的说明，告诉 ta「这是家里送来的消息，不是对话框里的人打的字，回复只能走 `bridge_reply`」。你家的规矩可以写进 `extraContext`。

## 测试

```
npm test
```
`test-bridge-replay.mjs` 在临时目录里起一个真的桥、模拟卡片和后端走完整流程（回复只交付一次、旧窗口抢不走新窗口的消息、重启后不丢不重、写盘失败不假装成功、交接卡、闹钟、换窗……）；`test-bridge-logic.mjs` 测队列和闹钟的纯逻辑。

## 许可

MIT。派生自 Sylvie 的 mcp-app-message-bridge（MIT），并借鉴了 ChatTG-Bridge 的一个思路，见 [NOTICE](NOTICE) 和 [LICENSE](LICENSE)。
