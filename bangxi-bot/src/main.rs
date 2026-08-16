use azalea::prelude::*;
use azalea::WalkDirection;
use azalea::protocol::packets::game::s_interact::InteractionHand;
use azalea::protocol::packets::game::s_swing::ServerboundSwing;
use axum::{extract::Query, extract::State, routing::get, routing::post, Json, Router};
use parking_lot::Mutex;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// 单个 bot 的运行时上下文（HTTP 层与游戏层共享）
#[derive(Clone, Component)]
struct BotContext {
    player: String,
    password: String,
    /// 待发送的聊天消息队列（HTTP → bot）
    queue: Arc<Mutex<Vec<String>>>,
    /// 登出标记（HTTP → bot）
    logout: Arc<AtomicBool>,
    /// 收到游戏内聊天时转发到的 webhook（None = 不转发；/set_chathook 设置）
    chathook: Arc<Mutex<Option<String>>>,
    /// 已发送认证命令（/login 或 /reg），防止重复发送（spawn 兜底与 Chat 事件共用）
    authenticated: Arc<AtomicBool>,
}

impl BotContext {
    fn new(player: String, password: String) -> Self {
        Self {
            player,
            password,
            queue: Arc::new(Mutex::new(Vec::new())),
            logout: Arc::new(AtomicBool::new(false)),
            chathook: Arc::new(Mutex::new(None)),
            authenticated: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Default for BotContext {
    fn default() -> Self {
        Self::new(String::new(), String::new())
    }
}

/// 所有 bot 的注册表（HTTP 层）
#[derive(Clone, Default)]
struct BotRegistry {
    bots: Arc<Mutex<HashMap<String, BotContext>>>,
}

/// HTTP 应用状态
#[derive(Clone)]
struct AppState {
    reg: BotRegistry,
    /// 创建 bot 的通道（HTTP → LocalSet）
    bot_tx: tokio::sync::mpsc::UnboundedSender<(String, BotContext)>,
}

/// HTTP 查询/请求结构
#[derive(Deserialize)]
struct LoginQuery {
    player: String,
    password: String,
}

#[derive(Deserialize)]
struct ChatQuery {
    player: String,
}

#[derive(Deserialize)]
struct ChatBody {
    message: String,
}

#[derive(Deserialize)]
struct LogoutQuery {
    player: String,
}

/// /set_chathook 的 query 参数（也可放 JSON body，见 SetChatHookBody）
#[derive(Deserialize)]
struct SetChatHookQuery {
    player: Option<String>,
    hookurl: Option<String>,
}

#[derive(Deserialize)]
struct SetChatHookBody {
    player: Option<String>,
    hookurl: Option<String>,
}

#[tokio::main]
async fn main() -> AppExit {
    let registry = BotRegistry::default();
    let (bot_tx, mut bot_rx) = tokio::sync::mpsc::unbounded_channel::<(String, BotContext)>();
    let app_state = AppState {
        reg: registry.clone(),
        bot_tx,
    };

    // HTTP 管理服务器（axum 内部用 tokio::spawn，跑在全局 runtime）
    tokio::spawn(async move {
        let app = Router::new()
            .route("/login", get(handle_login).post(handle_login))
            .route("/chat", post(handle_chat))
            .route("/logout", get(handle_logout).post(handle_logout))
            .route("/set_chathook", post(handle_set_chathook))
            .with_state(app_state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:7777")
            .await
            .unwrap();
        println!("[HTTP] Server listening on http://127.0.0.1:7777");
        axum::serve(listener, app).await.unwrap();
    });

    // LocalSet：启动 bot task（azalea 的 ClientBuilder::start() 是 !Send future，
    // 必须跑在 LocalSet 里）。通过 channel 接收 /login 的创建请求。
    let local = tokio::task::LocalSet::new();
    local.spawn_local(async move {
        let reg2 = registry.clone();
        while let Some((player, ctx)) = bot_rx.recv().await {
            let reg3 = reg2.clone();
            tokio::task::spawn_local(async move {
                start_bot(player.clone(), ctx).await;
                // bot 结束后从注册表移除，之后可重新 /login
                reg3.bots.lock().remove(&player);
                println!("[HTTP] Bot removed from registry: {}", player);
            });
        }
    });
    local.await;
    AppExit::Success
}

/// POST /login?player=xxx&password=yyy —— 创建并登录名为 xxx 的 bot
async fn handle_login(State(app): State<AppState>, Query(q): Query<LoginQuery>) -> &'static str {
    let player = q.player.clone();
    let password = q.password.clone();
    let ctx = BotContext::new(player.clone(), password);
    {
        let mut bots = app.reg.bots.lock();
        if bots.contains_key(&player) {
            return "already online";
        }
        bots.insert(player.clone(), ctx.clone());
    }
    println!("[HTTP] Login request: player={}", player);
    // 通知 LocalSet 启动该 bot
    let _ = app.bot_tx.send((player, ctx));
    "ok"
}

/// POST /chat?player=xxx  body {"message":"..."} —— 让名为 xxx 的 bot 在游戏里发言
async fn handle_chat(
    State(app): State<AppState>,
    Query(q): Query<ChatQuery>,
    Json(body): Json<ChatBody>,
) -> &'static str {
    let bots = app.reg.bots.lock();
    match bots.get(&q.player) {
        Some(ctx) => {
            ctx.queue.lock().push(body.message);
            "ok"
        }
        None => "bot not online",
    }
}

/// POST /logout?player=xxx —— 让名为 xxx 的 bot 断开登出
async fn handle_logout(
    State(app): State<AppState>,
    Query(q): Query<LogoutQuery>,
) -> &'static str {
    let bots = app.reg.bots.lock();
    match bots.get(&q.player) {
        Some(ctx) => {
            ctx.logout.store(true, Ordering::Relaxed);
            "ok"
        }
        None => "bot not online",
    }
}

/// POST /set_chathook —— 设置名为 player 的 bot 收到游戏内聊天时转发到的 webhook。
/// 参数 player / hookurl 支持 query 或 JSON body 两种传法：
///   curl -X POST "http://127.0.0.1:7777/set_chathook?player=xxx&hookurl=http://..."
///   curl -X POST http://127.0.0.1:7777/set_chathook -d '{"player":"xxx","hookurl":"http://..."}'
/// hookurl 留空 = 清除转发。bot 收到聊天后会向 hookurl POST form 表单 message=<聊天内容>。
async fn handle_set_chathook(
    State(app): State<AppState>,
    Query(q): Query<SetChatHookQuery>,
    body: Option<Json<SetChatHookBody>>,
) -> &'static str {
    let player = q.player.or_else(|| body.as_ref().and_then(|b| b.player.clone()));
    let hookurl = q
        .hookurl
        .or_else(|| body.as_ref().and_then(|b| b.hookurl.clone()));
    let (Some(player), Some(hookurl)) = (player, hookurl) else {
        return "bad request";
    };
    let bots = app.reg.bots.lock();
    match bots.get(&player) {
        Some(ctx) => {
            let hook = if hookurl.trim().is_empty() {
                None
            } else {
                Some(hookurl)
            };
            *ctx.chathook.lock() = hook;
            println!("[HTTP] Set chathook: player={}", player);
            "ok"
        }
        None => "bot not online",
    }
}

/// 从服务器消息中提取 /captcha 命令（含参数），找不到返回 None。
/// 例："请使用 /captcha 4f7a 完成验证" -> "/captcha 4f7a"
/// 例："发送 /captcha1234 完成验证"  -> "/captcha1234"
/// 提取规则：找到 /captcha 后连续收集 ASCII 字母数字/空白/斜杠，遇到中文等非 ASCII 即停。
fn extract_captcha_command(msg: &str) -> Option<String> {
    let lower = msg.to_lowercase();
    let idx = lower.find("/captcha")?;
    let cmd: String = msg[idx..]
        .chars()
        .take_while(|c| {
            c.is_ascii_alphanumeric() || c.is_ascii_whitespace() || *c == '/'
        })
        .collect();
    let cmd = cmd.trim().to_string();
    if cmd.is_empty() {
        None
    } else {
        Some(cmd)
    }
}

/// 启动一个 bot 客户端并保持在线
async fn start_bot(player: String, ctx: BotContext) {
    println!("[Bot:{}] Connecting to server...", player);
    let account = Account::offline(&player);
    let _exit = ClientBuilder::new()
        .set_handler(handle_event)
        .set_state(ctx)
        // 禁用自动重连：断开后 start() 返回，task 结束并从注册表移除，
        // 避免传送失败等情况下无限重连循环
        .reconnect_after(None)
        .start(account, "bx.bangxi.top")
        .await;
    println!("[Bot:{}] Bot task ended", player);
}

/// Bot 事件处理器
async fn handle_event(bot: Client, event: Event, ctx: BotContext) -> eyre::Result<()> {
    match event {
        Event::Spawn => {
            println!("[Bot:{}] Spawned! Starting auto-login sequence...", ctx.player);
            bot.wait_ticks(40).await;

            // ========== 步骤 1: 等待服务器认证提示，提示已到则跳过 ==========
            // 服务器 spawn 后通常会发 /captcha、/reg 或 /login 提示，Chat 事件检测到后
            // 已直接发送对应命令（并置位 authenticated）。这里等 3 秒（60 ticks）：
            // 提示已到则什么都不发（发过 /reg 就不再发 /login），没有提示则兜底发 /login
            // （兼容不发提示的服务器）。
            println!("[Bot:{}] Step 1: waiting for auth hint (up to 3s)...", ctx.player);
            bot.wait_ticks(60).await;
            if !ctx.authenticated.load(Ordering::Relaxed) {
                let cmd = format!("/login {}", ctx.password);
                ctx.authenticated.store(true, Ordering::Relaxed);
                println!("[Bot:{}] Step 1: no auth hint, fallback: {}", ctx.player, cmd);
                bot.chat(cmd);
            } else {
                println!(
                    "[Bot:{}] Step 1: auth command already sent via chat event",
                    ctx.player
                );
            }
            bot.wait_ticks(80).await; // 等待服务器处理登录命令

            // ========== 步骤 2: 前进 2 格并面向正前方（水平），右键手里的钟表 ==========
            // 菜单插件依赖完整的右键动作触发，且单次右键有时不被服务器接受（时好时坏）。
            // 先补发 ServerboundSwing（挥臂），等 100ms 再 start_use_item，
            // 并检测菜单是否真的打开（玩家物品栏 46 格 = 没打开），没开就重试。
            println!("[Bot:{}] Step 2: Walking forward 2 blocks, then right-clicking clock", ctx.player);
            let start = bot.position()?;
            bot.walk(WalkDirection::Forward);
            let mut walked = false;
            for _ in 0..120 {
                // 最多 6 秒，水平位移达到 2 格就停
                bot.wait_ticks(1).await;
                let p = bot.position()?;
                let dx = p.x - start.x;
                let dz = p.z - start.z;
                if dx * dx + dz * dz >= 4.0 {
                    walked = true;
                    break;
                }
            }
            bot.walk(WalkDirection::None);
            if !walked {
                println!("[Bot:{}] Warn: didn't walk 2 blocks (stuck?), proceeding anyway", ctx.player);
            }
            bot.wait_ticks(5).await; // 停稳

            // 菜单是否已打开：玩家自己的物品栏固定 46 格，打开容器后格数不同
            let menu_open = |bot: &Client| -> bool {
                match bot.get_inventory() {
                    Ok(c) => match c.contents() {
                        Some(contents) => contents.len() != 46,
                        None => false,
                    },
                    Err(_) => false,
                }
            };

            let mut menu_opened = false;
            for attempt in 0..4 {
                // 面向正前方：保持当前 yaw，pitch 置 0（水平）
                let yaw = {
                    let d = bot.direction()?;
                    d.y_rot()
                };
                bot.set_direction(yaw, 0.0)?;
                bot.wait_ticks(10).await; // 0.5 秒等视角生效
                // 完整右键：先挥臂（swing），等 100ms 让 swing 先到服务器，再使用物品
                bot.write_packet(ServerboundSwing {
                    hand: InteractionHand::MainHand,
                });
                bot.wait_ticks(2).await;
                bot.start_use_item();
                // 等最多 3 秒确认菜单打开
                for _ in 0..3 {
                    bot.wait_ticks(20).await; // 1 秒
                    if menu_open(&bot) {
                        menu_opened = true;
                        break;
                    }
                }
                if menu_opened {
                    println!("[Bot:{}] Menu opened after right-click (attempt {})", ctx.player, attempt);
                    break;
                }
                println!(
                    "[Bot:{}] Menu not opened yet, retry right-click (attempt {})",
                    ctx.player,
                    attempt + 1
                );
            }
            if !menu_opened {
                // 4 次完整右键都没打开菜单，断开本 bot（不杀进程，不影响其他 bot）
                println!("[Bot:{}] FATAL: menu did not open, disconnecting", ctx.player);
                bot.disconnect();
                return Ok(());
            }

            // ========== 步骤 3: 轮询容器，等菜单内容加载，找到信标槽位 ==========
            let mut beacon_slot: Option<usize> = None;
            for attempt in 0..8 {
                bot.wait_ticks(20).await; // 1 秒
                match bot.get_inventory() {
                    Ok(container) => {
                        if let Some(title) = container.title() {
                            println!("[Bot:{}] Container opened: {}", ctx.player, title.to_string());
                        }
                        if let Some(contents) = container.contents() {
                            println!(
                                "[Bot:{}] Container has {} slots (attempt {})",
                                ctx.player,
                                contents.len(),
                                attempt
                            );
                            for (index, slot) in contents.iter().enumerate() {
                                if let azalea::inventory::ItemStack::Present(item) = slot {
                                    println!("[Bot:{}] Slot {}: {:?}", ctx.player, index, item.kind);
                                    if item.kind == azalea::registry::builtin::ItemKind::Beacon {
                                        beacon_slot = Some(index);
                                        break;
                                    }
                                }
                            }
                            if beacon_slot.is_some() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        println!("[Bot:{}] Failed to get inventory: {:?}", ctx.player, e);
                    }
                }
            }

            match beacon_slot {
                Some(slot) => {
                    // ========== 步骤 4: 左键点击信标并验证传送（位置变化） ==========
                    println!(
                        "[Bot:{}] Found beacon at slot {}, clicking (left-click)...",
                        ctx.player, slot
                    );
                    let mut teleported = false;
                    for click_try in 0..6 {
                        let before = bot.position()?;
                        if let Ok(container) = bot.get_inventory() {
                            container.left_click(slot);
                        }
                        bot.wait_ticks(40).await; // 2 秒
                        let p = bot.position()?;
                        let win = match bot.get_inventory() {
                            Ok(c) => Some(c.id()),
                            Err(_) => None,
                        };
                        println!(
                            "[Bot:{}] click#{} pos=({:.1},{:.1}) window={:?}",
                            ctx.player,
                            click_try + 1,
                            p.x,
                            p.z,
                            win
                        );
                        let dx = p.x - before.x;
                        let dz = p.z - before.z;
                        if dx * dx + dz * dz > 0.25 {
                            // 水平位移超过 0.5 格，认为传送成功
                            teleported = true;
                            println!(
                                "[Bot:{}] Teleported to ({}, {}), after click #{}",
                                ctx.player,
                                p.x as i32,
                                p.z as i32,
                                click_try + 1
                            );
                            break;
                        }
                        println!(
                            "[Bot:{}] Teleport not detected after click #{}, retrying...",
                            ctx.player,
                            click_try + 1
                        );
                    }
                    if !teleported {
                        // 传送没触发（如账号被封禁等），断开本 bot，可重新 /login
                        println!("[Bot:{}] FATAL: teleport did not trigger, disconnecting", ctx.player);
                        bot.disconnect();
                        return Ok(());
                    }
                }
                None => {
                    // 兜底：8 秒内始终没找到信标，点容器中央（保持原行为）
                    if let Ok(container) = bot.get_inventory() {
                        if let Some(contents) = container.contents() {
                            let center_index = match contents.len() {
                                27 => 13,  // 9x3 箱子菜单正中央
                                54 => 31,  // 9x6 大箱子正中央
                                n => n / 2, // 其他大小，取中间
                            };
                            println!(
                                "[Bot:{}] Beacon not found by scanning, clicking center slot {}",
                                ctx.player, center_index
                            );
                            if center_index < contents.len() {
                                container.left_click(center_index);
                            }
                        }
                    }
                }
            }

            println!("[Bot:{}] Auto-login sequence completed!", ctx.player);
        }

        Event::Tick => {
            // 登出标记
            if ctx.logout.load(Ordering::Relaxed) {
                println!("[Bot:{}] Logout requested, disconnecting", ctx.player);
                bot.disconnect();
                return Ok(());
            }
            // 消费消息队列（限速 3 秒/条、每轮最多 5 条，防止高频调用导致游戏内刷屏封号）
            let messages: Vec<String> = std::mem::take(&mut *ctx.queue.lock());
            for msg in messages.into_iter().take(5) {
                println!("[Bot:{}] Sending chat: {}", ctx.player, msg);
                bot.chat(msg);
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }

        Event::Chat(m) => {
            // 打印收到的聊天消息（调试用）
            let (sender, content) = m.split_sender_and_content();
            // 服务器提示 /reg（如"请先使用 /reg 注册"）：若还没认证，发 /reg <pw> <pw>。
            // 注册成功后插件一般自动登录，因此发过 /reg 就不再发 /login（authenticated 置位）。
            if content.contains("/reg") && !ctx.authenticated.load(Ordering::Relaxed) {
                let cmd = format!("/reg {} {}", ctx.password, ctx.password);
                ctx.authenticated.store(true, Ordering::Relaxed);
                println!("[Bot:{}] Server requested /reg, sending: {}", ctx.player, cmd);
                bot.chat(cmd);
            }
            // 服务器提示 /login（如"请输入密码以登录"）：若还没认证，发 /login <pw>
            if content.contains("/login") && !ctx.authenticated.load(Ordering::Relaxed) {
                let cmd = format!("/login {}", ctx.password);
                ctx.authenticated.store(true, Ordering::Relaxed);
                println!("[Bot:{}] Server requested /login, sending: {}", ctx.player, cmd);
                bot.chat(cmd);
            }
            // 服务器要求 /captcha 验证：从消息里提取完整命令（含验证码）并立即发送
            if let Some(cmd) = extract_captcha_command(&content) {
                println!("[Bot:{}] Server requested captcha, sending: {}", ctx.player, cmd);
                bot.chat(cmd);
            }
            if let Some(sender) = sender {
                if sender != bot.username() {
                    println!("[Chat:{}] {}: {}", ctx.player, sender, content);
                    // 设置了 chathook 时，把收到的聊天 POST 到 hook（异步，不阻塞 bot 事件循环）
                    let hook = ctx.chathook.lock().clone();
                    if let Some(hook) = hook {
                        let player = ctx.player.clone();
                        tokio::spawn(async move {
                            let res = reqwest::Client::new()
                                .post(&hook)
                                .form(&[("message", content)])
                                .send()
                                .await;
                            match res {
                                Ok(r) => println!(
                                    "[ChatHook:{}] POST {} -> {}",
                                    player,
                                    hook,
                                    r.status()
                                ),
                                Err(e) => println!(
                                    "[ChatHook:{}] POST {} failed: {}",
                                    player,
                                    hook,
                                    e
                                ),
                            }
                        });
                    }
                }
            }
        }

        Event::Disconnect(reason) => {
            println!(
                "[Bot:{}] Disconnected: {:?}",
                ctx.player,
                reason.as_ref().map(|r| r.to_string())
            );
            // 已禁用自动重连，断开即结束：退出 client，让 start() 返回，
            // 从而移除注册表（之后可重新 /login）
            bot.exit();
        }

        _ => {}
    }

    Ok(())
}
