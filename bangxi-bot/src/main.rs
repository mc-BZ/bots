use azalea::prelude::*;
use azalea::core::position::Vec3;
use axum::{extract::State, routing::post, Json, Router};
use parking_lot::Mutex;
use serde::Deserialize;
use std::sync::Arc;

/// 共享状态，用于 HTTP 服务器和 Bot 之间通信
#[derive(Clone, Component)]
struct SharedState {
    /// 待发送的聊天消息队列
    pending_chat: Arc<Mutex<Vec<String>>>,
    /// 标记是否已完成自动登录
    login_done: Arc<Mutex<bool>>,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            pending_chat: Arc::new(Mutex::new(Vec::new())),
            login_done: Arc::new(Mutex::new(false)),
        }
    }
}

/// HTTP 请求体
#[derive(Deserialize)]
struct ChatRequest {
    message: String,
}

/// HTTP 应用状态
#[derive(Clone)]
struct AppState {
    shared: SharedState,
}

#[tokio::main]
async fn main() -> AppExit {
    // 创建共享状态
    let shared = SharedState::default();
    let app_state = AppState {
        shared: shared.clone(),
    };

    // 启动 HTTP 服务器（在后台 task 中）
    tokio::spawn(async move {
        let app = Router::new()
            .route("/chat", post(handle_chat))
            .with_state(app_state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:7777").await.unwrap();
        println!("[HTTP] Server listening on http://127.0.0.1:7777");
        axum::serve(listener, app).await.unwrap();
    });

    // 启动 Minecraft Bot
    let account = Account::offline("bzbot");
    // 如果要使用正版账号，取消下面一行的注释：
    // let account = Account::microsoft("your_email@example.com").await.unwrap();

    println!("[Bot] Connecting to server...");

    ClientBuilder::new()
        .set_handler(handle)
        .set_state(shared)
        .start(account, "bx.bangxi.top")
        .await
}

/// HTTP POST /chat 处理器
async fn handle_chat(
    State(state): State<AppState>,
    Json(req): Json<ChatRequest>,
) -> &'static str {
    println!("[HTTP] Received chat request: {}", req.message);
    state.shared.pending_chat.lock().push(req.message);
    "ok"
}

/// Bot 事件处理器
async fn handle(bot: Client, event: Event, state: SharedState) -> eyre::Result<()> {
    match event {
        Event::Spawn => {
            // 只在首次生成时执行登录流程
            {
                let mut login_done = state.login_done.lock();
                if *login_done {
                    return Ok(());
                }
                *login_done = true;
            }

            println!("[Bot] Spawned! Starting auto-login sequence...");

            // 等待服务器稳定
            bot.wait_ticks(40).await;

            // ========== 步骤 1: 输入 /login xxx ==========
            println!("[Bot] Step 1: Sending /login command");
            bot.chat("/login Bbsw2013");

            // 等待服务器处理登录命令（放宽到 4 秒）
            bot.wait_ticks(80).await;

            // ========== 步骤 2: 抬头看天空（确保准星 Miss），右键手里的钟表 ==========
            // start_use_item 的行为取决于准星：指向方块/实体会变成对方块/实体右键，
            // 菜单不会打开。先看向天空，保证发送的是「使用物品」。
            println!("[Bot] Step 2: Looking up, then right-clicking clock in hand");
            let pos = bot.position()?;
            bot.look_at(Vec3::new(pos.x, pos.y + 100.0, pos.z));
            bot.wait_ticks(10).await; // 0.5 秒等视角生效
            bot.start_use_item();

            // ========== 步骤 3: 轮询容器，等菜单内容加载后点击信标 ==========
            // 菜单内容不会立刻同步，逐秒重试，最多 8 次（8 秒）
            let mut clicked = false;
            for attempt in 0..8 {
                bot.wait_ticks(20).await; // 1 秒
                match bot.get_inventory() {
                    Ok(container) => {
                        if let Some(title) = container.title() {
                            println!("[Bot] Container opened: {}", title.to_string());
                        }
                        if let Some(contents) = container.contents() {
                            println!(
                                "[Bot] Container has {} slots (attempt {})",
                                contents.len(),
                                attempt
                            );
                            let mut found_beacon = false;
                            for (index, slot) in contents.iter().enumerate() {
                                if let azalea::inventory::ItemStack::Present(item) = slot {
                                    println!("[Bot] Slot {}: {:?}", index, item.kind);
                                    if item.kind == azalea::registry::builtin::ItemKind::Beacon {
                                        println!("[Bot] Found beacon at slot {}, clicking...", index);
                                        container.left_click(index);
                                        clicked = true;
                                        found_beacon = true;
                                        break;
                                    }
                                }
                            }
                            if found_beacon {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        println!("[Bot] Failed to get inventory: {:?}", e);
                    }
                }
            }

            // 兜底：8 秒内始终没找到信标，点容器中央（保持原行为）
            if !clicked {
                if let Ok(container) = bot.get_inventory() {
                    if let Some(contents) = container.contents() {
                        let center_index = match contents.len() {
                            27 => 13,  // 9x3 箱子菜单正中央
                            54 => 31,  // 9x6 大箱子正中央
                            n => n / 2, // 其他大小，取中间
                        };
                        println!(
                            "[Bot] Beacon not found by scanning, clicking center slot {}",
                            center_index
                        );
                        if center_index < contents.len() {
                            container.left_click(center_index);
                        }
                    }
                }
            }

            println!("[Bot] Auto-login sequence completed!");
        }

        Event::Tick => {
            // 检查是否有待发送的 HTTP 消息
            let messages: Vec<String> = std::mem::take(&mut *state.pending_chat.lock());
            for msg in messages {
                println!("[Bot] Sending chat from HTTP: {}", msg);
                bot.chat(msg);
            }
        }

        Event::Chat(m) => {
            // 打印收到的聊天消息（调试用）
            let (sender, content) = m.split_sender_and_content();
            if let Some(sender) = sender {
                if sender != bot.username() {
                    println!("[Chat] {}: {}", sender, content);
                }
            }
        }

        Event::Disconnect(reason) => {
            println!(
                "[Bot] Disconnected: {:?}",
                reason.as_ref().map(|r| r.to_string())
            );
        }

        _ => {}
    }

    Ok(())
}