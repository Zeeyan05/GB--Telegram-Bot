require('dotenv').config();
console.log('Mongo URI:', process.env.MONGO_URI);

// Environment validation
const REQUIRED_ENV_VARS = ['BOT_TOKEN', 'MONGO_URI', 'ADMIN_IDS', 'RPC_URL', 'CONTRACT_ADDRESS', 'PRIVATE_KEY', 'PAYOUT_CHANNEL_ID'];
const missingVars = REQUIRED_ENV_VARS.filter(varName => !process.env[varName]);
if (missingVars.length > 0) {
  console.error(`❌ Missing required environment variables: ${missingVars.join(', ')}`);
  process.exit(1);
}

const { Telegraf, Scenes, session, Markup } = require('telegraf');
const { MongoClient } = require('mongodb');
const { Web3 } = require('web3');
const axios = require('axios');
const fs = require('fs');
const csv = require('csv-parser');

// Initialize Web3
const web3 = new Web3(process.env.RPC_URL);
const contractABI = require('./contractABI.json');
const contract = new web3.eth.Contract(contractABI, process.env.CONTRACT_ADDRESS);
const adminWallet = web3.eth.accounts.privateKeyToAccount(process.env.PRIVATE_KEY);
web3.eth.accounts.wallet.add(adminWallet);

// Configuration
const config = {
  BOT_TOKEN: process.env.BOT_TOKEN,
  MONGO_URI: process.env.MONGO_URI,
  ADMIN_IDS: JSON.parse(process.env.ADMIN_IDS),
  LINKS: {
    channel: 'https://t.me/Gigabyteannouncement',
    promoterchannel: 'https://t.me/captainofcryptos',
    promoterchannel2: 'https://t.me/Wurve',
    group: 'https://t.me/GigabyteSupportChat',
    promotergroup: 'https://t.me/CaptainOfCryptoTeam',
    paymentchannel: 'https://t.me/payoutworld_1',
    twitter: 'https://x.com/captofcrypto',
    twitter2: 'https://x.com/GigabyteAirdrop',
  },
  REQUIRED_GROUPS: ['@Gigabyteannouncement', '@GigabyteSupportChat', '@captainofcryptos', '@CaptainOfCryptoTeam'],
  MIN_WITHDRAW_AMOUNT: 15000,
  REFERRAL_REWARD: 5000,
  PAYOUT_CHANNEL: '@payoutworld_1',
  GAS_LIMIT: 200000,
  TOKEN_DECIMALS: 18,
  AUTO_WITHDRAW_INTERVAL: 5 * 60 * 1000,
  MAX_RETRIES: 3,
  MAX_BULK_ACTIONS: 100,
  DAILY_WITHDRAW_LIMIT: 50000,
  TOKEN_INFO: {
    name: 'Gigabyte Token',
    symbol: 'GB',
    contractAddress: process.env.CONTRACT_ADDRESS,
    decimals: 18,
    totalSupply: '1,000,000,000 GB',
    network: 'Polygon',
    explorer: `https://polygonscan.com/token/${process.env.CONTRACT_ADDRESS}`
  }
};

// Initialize
const bot = new Telegraf(config.BOT_TOKEN);
const mongoClient = new MongoClient(config.MONGO_URI);
let db;

// Session management
bot.use(session({
  defaultSession: () => ({
    state: null,
    data: {},
    adminAction: null,
    withdrawalData: null,
    verifyMsgId: null,
    userList: {
      page: 0,
      type: 'balance',
      data: []
    },
    bulkAction: {
      type: null,
      users: [],
      currentIndex: 0
    },
    bulkMessage: null,
    bulkMessageTarget: null,
    airdropData: null,
    currentUser: null
  })
}));

// ==================== [SCENES] ====================

const captchaScene = new Scenes.BaseScene('captcha');
captchaScene.enter(ctx => {
  const a = Math.floor(Math.random() * 10 + 10);
  const b = Math.floor(Math.random() * 10 + 10);
  ctx.scene.session.answer = a + b;
  ctx.reply(`🤖 Solve: ${a} + ${b} = ?`);
});

captchaScene.on('text', async ctx => {
  if (parseInt(ctx.message.text) === ctx.scene.session.answer) {
    await db.collection('verified').updateOne(
      { userId: ctx.from.id },
      { $set: { verified: true } },
      { upsert: true }
    );
    ctx.reply('✅ Verified!');
    ctx.scene.leave();
    return showJoinMessage(ctx);
  }
  ctx.reply('❌ Incorrect, try again.');
});

const userListScene = new Scenes.BaseScene('userListScene');
userListScene.enter(async (ctx) => {
  ctx.session.userList = {
    page: 0,
    type: ctx.scene.state.type || 'balance',
    data: []
  };

  await fetchUserListData(ctx);
  await displayUserListPage(ctx);
});

const bulkActionScene = new Scenes.BaseScene('bulkAction');
bulkActionScene.enter(async (ctx) => {
  const action = ctx.scene.state.action;
  ctx.session.bulkAction = {
    type: action,
    users: [],
    currentIndex: 0
  };

  await ctx.reply(`📦 <b>Bulk ${action.charAt(0).toUpperCase() + action.slice(1)}</b>\n\n` +
    `Please upload a CSV file with user IDs in the first column${action.includes('token') ? ' and amounts in the second column' : ''}.\n\n` +
    `Or send user IDs separated by commas:`, {
    parse_mode: 'HTML',
    ...Markup.keyboard([['🔙 Cancel']]).resize()
  });
});

bulkActionScene.on('document', async (ctx) => {
  try {
    const fileId = ctx.message.document.file_id;
    const fileLink = await ctx.telegram.getFileLink(fileId);
    const users = [];
    
    await new Promise((resolve, reject) => {
      axios.get(fileLink, { responseType: 'stream' })
        .then(response => {
          response.data
            .pipe(csv())
            .on('data', (row) => {
              const userId = parseInt(Object.values(row)[0]);
              const amount = ctx.session.bulkAction.type.includes('token') ? parseInt(Object.values(row)[1] || 0) : 0;
              if (userId) users.push({ userId, amount });
            })
            .on('end', resolve)
            .on('error', reject);
        });
    });

    if (users.length > config.MAX_BULK_ACTIONS) {
      return ctx.reply(`❌ Maximum ${config.MAX_BULK_ACTIONS} users allowed in bulk action.`);
    }

    ctx.session.bulkAction.users = users;
    await confirmBulkAction(ctx);
  } catch (e) {
    console.error('Bulk action CSV error:', e);
    ctx.reply('❌ Error processing CSV file. Please check the format and try again.');
  }
});

bulkActionScene.on('text', async (ctx) => {
  if (ctx.message.text === '🔙 Cancel') {
    ctx.session.bulkAction = null;
    return ctx.scene.leave();
  }

  try {
    const users = ctx.message.text.split(',').map(item => {
      const parts = item.trim().split(' ');
      const userId = parseInt(parts[0]);
      const amount = ctx.session.bulkAction.type.includes('token') ? parseInt(parts[1] || 0) : 0;
      return { userId, amount };
    }).filter(u => u.userId);

    if (users.length > config.MAX_BULK_ACTIONS) {
      return ctx.reply(`❌ Maximum ${config.MAX_BULK_ACTIONS} users allowed in bulk action.`);
    }

    ctx.session.bulkAction.users = users;
    await confirmBulkAction(ctx);
  } catch (e) {
    ctx.reply('❌ Invalid format. Please provide user IDs separated by commas.');
  }
});

async function confirmBulkAction(ctx) {
  const { type, users } = ctx.session.bulkAction;
  await ctx.reply(`📋 <b>Bulk Action Summary</b>\n\n` +
    `Type: ${type}\n` +
    `Users: ${users.length}\n` +
    `First 5: ${users.slice(0, 5).map(u => u.userId).join(', ')}${users.length > 5 ? '...' : ''}\n\n` +
    `Proceed with this action?`, {
    parse_mode: 'HTML',
    ...Markup.keyboard([
      ['✅ Confirm Bulk Action'],
      ['🔙 Cancel']
    ]).resize()
  });
}

bulkActionScene.hears('✅ Confirm Bulk Action', async (ctx) => {
  const { type, users } = ctx.session.bulkAction;
  let successCount = 0;
  let failCount = 0;
  const errors = [];

  await ctx.reply(`⏳ Processing ${users.length} users...`);

  for (const user of users) {
    try {
      if (type === 'ban') {
        await db.collection('users').updateOne(
          { userId: user.userId },
          { $set: { isBanned: true, banDate: new Date() } },
          { upsert: true }
        );
      } 
      else if (type === 'unban') {
        await db.collection('users').updateOne(
          { userId: user.userId },
          { $set: { isBanned: false, banDate: null } }
        );
      }
      else if (type === 'add_tokens') {
        await db.collection('balances').updateOne(
          { userId: user.userId },
          { $inc: { tokens: user.amount } },
          { upsert: true }
        );
      }
      else if (type === 'remove_tokens') {
        await db.collection('balances').updateOne(
          { userId: user.userId },
          { $inc: { tokens: -user.amount } },
          { upsert: true }
        );
      }
      successCount++;
    } catch (e) {
      failCount++;
      errors.push(`User ${user.userId}: ${e.message}`);
    }
  }

  let resultMsg = `✅ Bulk action completed!\n\n` +
    `Success: ${successCount}\n` +
    `Failed: ${failCount}`;

  if (failCount > 0) {
    resultMsg += `\n\nErrors:\n${errors.slice(0, 5).join('\n')}`;
    if (errors.length > 5) resultMsg += `\n...and ${errors.length - 5} more`;
  }

  await ctx.reply(resultMsg);
  ctx.session.bulkAction = null;
  ctx.scene.leave();
});

const stage = new Scenes.Stage([captchaScene, userListScene, bulkActionScene]);
bot.use(stage.middleware());

// ==================== [HELPER FUNCTIONS] ====================

function isAdmin(userId) {
  return config.ADMIN_IDS.includes(Number(userId));
}

async function showMainMenu(ctx) {
  const buttons = [
    ['↕️ Statistics', '💸 Withdraw'],
    ['ℹ️ Information', '💰 Token Info']
  ];
  
  if (isAdmin(ctx.from.id)) {
    buttons.push(['🛠️ Admin Panel']);
  }
  
  await ctx.reply('Main Menu:', Markup.keyboard(buttons).resize());
}

async function showAdminPanel(ctx) {
  if (!isAdmin(ctx.from.id)) return;
  
  try {
    const stats = await getSystemStats();
    await ctx.reply(`🛠️ <b>Admin Control Center</b>\n\n` +
      `👥 Users: ${stats.totalUsers}\n` +
      `💰 Tokens: ${stats.totalTokens}\n` +
      `⏳ Active: ${stats.activeUsers}\n` +
      `⏳ Pending Withdrawals: ${stats.pendingWithdrawals}\n` +
      `✅ Completed Withdrawals: ${stats.completedWithdrawals}`, {
      parse_mode: 'HTML',
      ...Markup.keyboard([
        ['📊 User Analytics', '💰 Token Operations'],
        ['🔍 Find User', '📜 User List (Top 50)'],
        ['📜 Top Referrers', '📩 Bulk Message'],
        ['🛡️ Ban Tools', '📦 Bulk Actions'],
        ['🏠 Main Menu']
      ]).resize()
    });
  } catch (e) {
    console.error('Admin panel error:', e);
    ctx.reply('❌ Error loading admin panel. Please try again.');
  }
}

async function getSystemStats() {
  const [totalUsers, activeUsers, totalTokens, bannedUsers, recentBans, pendingWithdrawals, completedWithdrawals] = await Promise.all([
    db.collection('users').countDocuments(),
    db.collection('users').countDocuments({ lastActive: { $gt: new Date(Date.now() - 86400000) } }),
    db.collection('balances').aggregate([{ $group: { _id: null, total: { $sum: "$tokens" } } }]).toArray(),
    db.collection('users').countDocuments({ isBanned: true }),
    db.collection('users').countDocuments({ isBanned: true, banDate: { $gt: new Date(Date.now() - 86400000) } }),
    db.collection('withdrawals').countDocuments({ status: 'pending' }),
    db.collection('withdrawals').countDocuments({ status: 'completed' })
  ]);
  
  return {
    totalUsers,
    activeUsers,
    totalTokens: totalTokens[0]?.total || 0,
    bannedUsers,
    recentBans,
    pendingWithdrawals,
    completedWithdrawals
  };
}

async function fetchUserListData(ctx) {
  try {
    if (ctx.session.userList.type === 'balance') {
      ctx.session.userList.data = await db.collection('balances')
        .find()
        .sort({ tokens: -1 })
        .limit(500)
        .toArray();
    } else {
      ctx.session.userList.data = await db.collection('users')
        .aggregate([
          { $match: { referrerId: { $exists: true } } },
          { $group: { _id: "$referrerId", count: { $sum: 1 } } },
          { $sort: { count: -1 } },
          { $limit: 500 }
        ])
        .toArray();
    }
  } catch (e) {
    console.error('Error fetching user list:', e);
    await ctx.reply('❌ Error loading user data. Please try again.');
    return ctx.scene.leave();
  }
}

async function displayUserListPage(ctx) {
  const { page, type, data } = ctx.session.userList;
  const itemsPerPage = 10;
  const startIdx = page * itemsPerPage;
  const endIdx = startIdx + itemsPerPage;
  const pageData = data.slice(startIdx, endIdx);
  const totalPages = Math.ceil(data.length / itemsPerPage);

  let message = `📜 <b>Top Users by ${type === 'balance' ? 'Token Balance' : 'Referrals'}</b>\n\n`;
  let counter = startIdx + 1;

  if (type === 'balance') {
    pageData.forEach(user => {
      message += `${counter++}. 👤 ${user.userId} - 💰 ${user.tokens} GB\n`;
    });
  } else {
    pageData.forEach(user => {
      message += `${counter++}. 👤 ${user._id} - 👥 ${user.count} referrals\n`;
    });
  }

  message += `\nPage ${page + 1} of ${totalPages}`;

  const keyboard = [];
  if (page > 0) keyboard.push(Markup.button.callback('⬅️ Previous', 'prev_page'));
  if (endIdx < data.length) keyboard.push(Markup.button.callback('Next ➡️', 'next_page'));
  
  keyboard.push(Markup.button.callback('📤 Export CSV', 'export_csv'));
  keyboard.push(Markup.button.callback('🔙 Admin Menu', 'back_to_admin'));

  await ctx.replyWithHTML(message, Markup.inlineKeyboard([keyboard]));
}

userListScene.action('prev_page', async (ctx) => {
  ctx.session.userList.page--;
  await ctx.answerCbQuery();
  await displayUserListPage(ctx);
});

userListScene.action('next_page', async (ctx) => {
  ctx.session.userList.page++;
  await ctx.answerCbQuery();
  await displayUserListPage(ctx);
});

userListScene.action('export_csv', async (ctx) => {
  await ctx.answerCbQuery('Generating CSV...');
  
  try {
    const { type, data } = ctx.session.userList;
    let csvContent = '';
    
    if (type === 'balance') {
      csvContent = 'Rank,User ID,Token Balance\n';
      data.forEach((user, index) => {
        csvContent += `${index + 1},${user.userId},${user.tokens}\n`;
      });
    } else {
      csvContent = 'Rank,User ID,Referral Count\n';
      data.forEach((user, index) => {
        csvContent += `${index + 1},${user._id},${user.count}\n`;
      });
    }
    
    await ctx.replyWithDocument({
      source: Buffer.from(csvContent),
      filename: `top_users_${type}_${new Date().toISOString().split('T')[0]}.csv`
    });
    
  } catch (e) {
    console.error('CSV export error:', e);
    await ctx.reply('❌ Failed to generate CSV. Please try again.');
  }
});

userListScene.action('back_to_admin', async (ctx) => {
  await ctx.answerCbQuery();
  await ctx.scene.leave();
  await showAdminPanel(ctx);
});

async function showJoinMessage(ctx, isStart = false) {
  const message = `🚀 *Welcome to Gigabyte Airdrop!*

💎 *Prize Pool:* 100,000 GB
🎁 *Top Referrers + Random Winners!*

🏆 *Referral Rewards:* 
1st — 100000 GB
2nd — 50000 GB
3rd — 30000 GB
4th-10th — 20000 GB each
11th-30th — 10000 GB each
31st-75th — 5000 GB each

📅 *End Date:* 1st june 2025
🚚 *Distribution Date:* 2nd june 2025

📋 *Mandatory Tasks:*
🔘 Join GigaByte [Channel](${config.LINKS.channel}) & [Group](${config.LINKS.group})
🔘 Follow GigBbyte [Twitter](${config.LINKS.twitter2})
🔘 Join promoter [Channel 1](${config.LINKS.promoterchannel}) & [Channel2](${config.LINKS.promoterchannel2})
🔘 Follow [Twitter](${config.LINKS.twitter})
🔘 Join Payment [channel](${config.LINKS.paymentchannel}) [(Optional)]

✅ Click "Check" to verify your entry and continue.`;

  if (isStart) {
    await ctx.replyWithMarkdown(message, Markup.inlineKeyboard([
      [Markup.button.callback('✅ Check', 'verify_join')]
    ]));
  } else {
    await showMainMenu(ctx);
  }
}

// ==================== [WITHDRAWAL PROCESSING] ====================

async function processWithdrawals() {
  try {
    const pendingWithdrawals = await db.collection('withdrawals')
      .find({ 
        status: 'pending',
        $or: [
          { retryCount: { $exists: false } },
          { retryCount: { $lt: config.MAX_RETRIES } }
        ]
      })
      .sort({ date: 1 })
      .limit(5)
      .toArray();

    if (pendingWithdrawals.length === 0) {
      console.log('No pending withdrawals to process');
      return;
    }

    console.log(`Processing ${pendingWithdrawals.length} withdrawals...`);

    for (const withdrawal of pendingWithdrawals) {
      try {
        const amountWei = web3.utils.toWei(
          withdrawal.amount.toString(), 
          'ether'
        );

        const txData = contract.methods.transfer(
          withdrawal.wallet,
          amountWei
        ).encodeABI();

        const txObject = {
          from: adminWallet.address,
          to: process.env.CONTRACT_ADDRESS,
          gas: config.GAS_LIMIT,
          data: txData
        };

        const gasPrice = await web3.eth.getGasPrice();
        txObject.gasPrice = gasPrice;

        const signedTx = await web3.eth.accounts.signTransaction(
          txObject,
          process.env.PRIVATE_KEY
        );

        const receipt = await web3.eth.sendSignedTransaction(
          signedTx.rawTransaction
        );

        await db.collection('withdrawals').updateOne(
          { _id: withdrawal._id },
          { 
            $set: { 
              status: 'completed',
              txHash: receipt.transactionHash,
              completedAt: new Date() 
            } 
          }
        );

        try {
          await bot.telegram.sendMessage(
            withdrawal.userId,
            `🎉 Your withdrawal of ${withdrawal.amount} GB has been processed!\n\n` +
            `📜 TX Hash: ${receipt.transactionHash}\n` +
            `🔗 View on explorer: https://polygonscan.com/tx/${receipt.transactionHash}`
          );
        } catch (e) {
          console.error('Failed to notify user:', e);
        }

        console.log(`Processed withdrawal for ${withdrawal.userId}: ${receipt.transactionHash}`);
        await new Promise(resolve => setTimeout(resolve, 5000));

      } catch (error) {
        console.error('Error processing withdrawal:', error);
        
        await db.collection('withdrawals').updateOne(
          { _id: withdrawal._id },
          { 
            $set: { 
              status: 'failed',
              error: error.message,
              retryCount: (withdrawal.retryCount || 0) + 1 
            } 
          }
        );

        if ((withdrawal.retryCount || 0) + 1 >= config.MAX_RETRIES) {
          try {
            await bot.telegram.sendMessage(
              config.ADMIN_IDS[0],
              `❌ Failed to process withdrawal after ${config.MAX_RETRIES} attempts:\n\n` +
              `User: ${withdrawal.userId}\n` +
              `Amount: ${withdrawal.amount}\n` +
              `Wallet: ${withdrawal.wallet}\n\n` +
              `Error: ${error.message}`
            );
          } catch (e) {
            console.error('Failed to notify admin:', e);
          }
        }
      }
    }
  } catch (error) {
    console.error('Error in withdrawal processing loop:', error);
  } finally {
    setTimeout(processWithdrawals, config.AUTO_WITHDRAW_INTERVAL);
  }
}

// ==================== [USER COMMANDS] ====================

bot.command('start', async ctx => {
  const verified = await db.collection('verified').findOne({ userId: ctx.from.id });
  if (!verified) {
    return ctx.scene.enter('captcha');
  }

  const startPayload = ctx.message.text.split(' ')[1];
  if (startPayload) {
    const referrerId = parseInt(startPayload);
    if (referrerId && referrerId !== ctx.from.id) {
      const existing = await db.collection('users').findOne({ userId: ctx.from.id });
      if (!existing || !existing.referrerId) {
        await db.collection('users').updateOne(
          { userId: ctx.from.id },
          { $set: { referrerId } },
          { upsert: true }
        );
        await db.collection('balances').updateOne(
          { userId: referrerId },
          { $inc: { tokens: config.REFERRAL_REWARD } },
          { upsert: true }
        );
        try {
          await ctx.telegram.sendMessage(
            referrerId,
            `🎉 You earned +${config.REFERRAL_REWARD} tokens for referring a new user!`
          );
        } catch (e) {
          console.log('Could not notify referrer');
        }
      }
    }
  }

  return showJoinMessage(ctx, true);
});

bot.command('menu', async ctx => {
  await showMainMenu(ctx);
});

bot.hears('↕️ Statistics', async ctx => {
  const userId = ctx.from.id;
  const user = await db.collection('users').findOne({ userId });
  const balance = await db.collection('balances').findOne({ userId });
  const referrals = await db.collection('users').countDocuments({ referrerId: userId });
  const withdrawals = await db.collection('withdrawals').find({ 
    userId,
    status: 'completed'
  }).sort({ completedAt: -1 }).limit(5).toArray();

  let withdrawalsText = '';
  if (withdrawals.length > 0) {
    withdrawalsText = '\n\n💸 Recent Withdrawals:\n' + withdrawals.map(w => 
      `${w.amount} GB - ${w.completedAt.toLocaleString()}`
    ).join('\n');
  }

  const stats = `🎁 *Gigabyte Airdrop Statistics:*

🏆 *Your Referrals:* ${referrals}
💰 *Your Balance:* ${balance?.tokens || 0} GB${withdrawalsText}

🔗 *Referral Link:* https://t.me/GigabyteAirdropBot?start=${userId}

📅 *Airdrop End:* 1st june 2025
🚀 *Top referral Distribution:* 2nd june 2025`;

  await ctx.replyWithMarkdown(stats);
});

bot.hears('💰 Token Info', async ctx => {
  const tokenInfo = `💎 *Gigabyte Token (GB) Information:*

🔹 *Name:* ${config.TOKEN_INFO.name}
🔹 *Symbol:* ${config.TOKEN_INFO.symbol}
🔹 *Contract Address:* \`${config.TOKEN_INFO.contractAddress}\`
🔹 *Decimals:* ${config.TOKEN_INFO.decimals}
🔹 *Total Supply:* ${config.TOKEN_INFO.totalSupply}
🔹 *Network:* ${config.TOKEN_INFO.network}
🔹 *Explorer:* [View on Polygonscan](${config.TOKEN_INFO.explorer})

📌 *Note:* GB is the native token of the Gigabyte ecosystem, powering all transactions and rewards.`;

  await ctx.replyWithMarkdown(tokenInfo);
});

bot.hears('ℹ️ Information', async ctx => {
  const info = `📘 *About Gigabyte:*

📑 Gigabyte (GB) is a next-generation decentralized financial ecosystem powered by Basr, creating seamless bridges between traditional and decentralized finance. Our platform features:

🚨 *Gigabyte Airdrop*  
🎁 *Prize Pool:* 1,000,000 GB  
🏆 *Winners:* 300 Random & Top 75 + referral bonus

🥇 1st — 100000 GB  
🥈 2nd — 50000 GB  
🥉 3rd — 30000 GB  
🔹 4th–10th — 20000 GB each  
🔹 11th–30th — 10000 GB each  
🔹 31st–75th — 5000 GB each

📅 *End Date:* 1st june 2025  
🚀 *Distribution:* 2nd june 2025

🔗 [Telegram Channel](https://t.me/Gigabyteannouncement) | [Telegram Group](https://t.me/GigabyteSupportChat) | [Twitter](https://x.com/GigabyteAirdrop)`;

  await ctx.replyWithMarkdown(info);
});

// ==================== [ADMIN COMMANDS] ====================

bot.hears('🛠️ Admin Panel', async (ctx) => {
  await showAdminPanel(ctx);
});

bot.hears('📊 User Analytics', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  
  try {
    const stats = await getSystemStats();
    await ctx.reply(`📈 <b>User Statistics</b>\n\n` +
      `👥 Total: ${stats.totalUsers}\n` +
      `🟢 Active: ${stats.activeUsers}\n` +
      `⛔ Banned: ${stats.bannedUsers}\n` +
      `⏳ Pending Withdrawals: ${stats.pendingWithdrawals}`, {
      parse_mode: 'HTML',
      ...Markup.keyboard([
        ['🔍 Find User', '📜 User List (Top 50)'],
        ['📊 Withdrawal Stats', '🔄 Refresh Stats'],
        ['🔙 Admin Menu']
      ]).resize()
    });
  } catch (e) {
    console.error('User analytics error:', e);
    ctx.reply('❌ Error loading user statistics. Please try again.');
  }
});

bot.hears('🔍 Find User', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  ctx.session.state = 'admin_find_user';
  await ctx.reply('Enter user ID to find:', Markup.keyboard([['🔙 Cancel']]).resize());
});

bot.hears('📜 User List (Top 50)', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  await ctx.scene.enter('userListScene', { type: 'balance' });
});

bot.hears('📜 Top Referrers', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  await ctx.scene.enter('userListScene', { type: 'referrals' });
});

bot.hears('💰 Token Operations', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  
  try {
    const stats = await getSystemStats();
    await ctx.reply(`💎 <b>Token Management</b>\n\n` +
      `Total tokens: ${stats.totalTokens}\n` +
      `Avg per user: ${Math.round(stats.totalTokens / stats.totalUsers)}`, {
      parse_mode: 'HTML',
      ...Markup.keyboard([
        ['➕ Add Tokens', '➖ Remove Tokens'],
        ['👥 Airdrop to All', '🟢 Airdrop to Active'],
        ['📋 Airdrop to List', '📊 Token Distribution'],
        ['🔙 Admin Menu']
      ]).resize()
    });
  } catch (e) {
    console.error('Token operations error:', e);
    ctx.reply('❌ Error loading token operations. Please try again.');
  }
});

bot.hears('➕ Add Tokens', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  ctx.session.state = 'admin_add';
  await ctx.reply('Enter user ID and amount to add (e.g., "12345 1000"):', Markup.keyboard([['🔙 Cancel']]).resize());
});

bot.hears('➖ Remove Tokens', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  ctx.session.state = 'admin_remove';
  await ctx.reply('Enter user ID and amount to remove (e.g., "12345 1000"):', Markup.keyboard([['🔙 Cancel']]).resize());
});

bot.hears('👥 Airdrop to All', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  ctx.session.state = 'admin_airdrop_all';
  await ctx.reply('Enter amount to airdrop to all users:', Markup.keyboard([['🔙 Cancel']]).resize());
});

bot.hears('🟢 Airdrop to Active', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  ctx.session.state = 'admin_airdrop_active';
  await ctx.reply('Enter amount to airdrop to active users (last 24h):', Markup.keyboard([['🔙 Cancel']]).resize());
});

bot.hears('📋 Airdrop to List', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  ctx.session.state = 'admin_airdrop_list';
  await ctx.reply('Send a CSV file with user IDs or list user IDs separated by commas:', Markup.keyboard([['🔙 Cancel']]).resize());
});

bot.hears('📊 Token Distribution', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  
  try {
    const distribution = await db.collection('balances')
      .aggregate([
        { 
          $bucket: {
            groupBy: "$tokens",
            boundaries: [0, 100, 1000, 5000, 10000, 50000, 100000, Infinity],
            default: "Other",
            output: {
              count: { $sum: 1 },
              total: { $sum: "$tokens" }
            }
          }
        }
      ])
      .toArray();
    
    const totalHolders = await db.collection('balances').countDocuments();
    const top10 = await db.collection('balances')
      .find()
      .sort({ tokens: -1 })
      .limit(10)
      .toArray();
    
    let distText = distribution.map(d => 
      `🏷️ ${d._id} GB: ${d.count} users (${((d.count / totalHolders) * 100).toFixed(1)}%)`
    ).join('\n');
    
    let top10Text = top10.map((u, i) => 
      `${i + 1}. 👤 ${u.userId}: ${u.tokens} GB`
    ).join('\n');
    
    await ctx.reply(`📊 <b>Token Distribution</b>\n\n` +
      `Total holders: ${totalHolders}\n\n` +
      `<b>Distribution:</b>\n${distText}\n\n` +
      `<b>Top 10 Holders:</b>\n${top10Text}`, {
      parse_mode: 'HTML',
      ...Markup.keyboard([['🔙 Admin Menu']]).resize()
    });
  } catch (e) {
    console.error('Token distribution error:', e);
    await ctx.reply('❌ Error loading token distribution.', Markup.keyboard([['🔙 Admin Menu']]).resize());
  }
});

bot.hears('📊 Withdrawal Stats', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  
  try {
    const [pending, completed, failed, totalAmount] = await Promise.all([
      db.collection('withdrawals').countDocuments({ status: 'pending' }),
      db.collection('withdrawals').countDocuments({ status: 'completed' }),
      db.collection('withdrawals').countDocuments({ status: 'failed' }),
      db.collection('withdrawals').aggregate([
        { $match: { status: 'completed' } },
        { $group: { _id: null, total: { $sum: "$amount" } }}
      ]).toArray()
    ]);
    
    await ctx.reply(`📊 <b>Withdrawal Statistics</b>\n\n` +
      `⏳ Pending: ${pending}\n` +
      `✅ Completed: ${completed}\n` +
      `❌ Failed: ${failed}\n` +
      `💰 Total Distributed: ${totalAmount[0]?.total || 0} GB`, {
      parse_mode: 'HTML',
      ...Markup.keyboard([
        ['📜 Recent Withdrawals', '📜 Failed Withdrawals'],
        ['🔄 Refresh Stats', '🔙 Admin Menu']
      ]).resize()
    });
  } catch (e) {
    console.error('Withdrawal stats error:', e);
    ctx.reply('❌ Error loading withdrawal statistics. Please try again.');
  }
});

bot.hears('📜 Recent Withdrawals', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  
  try {
    const withdrawals = await db.collection('withdrawals')
      .find({ status: 'completed' })
      .sort({ completedAt: -1 })
      .limit(10)
      .toArray();
    
    if (withdrawals.length === 0) {
      return ctx.reply('No recent withdrawals found.', Markup.keyboard([
        ['🔙 Admin Menu']
      ]).resize());
    }
    
    const text = withdrawals.map(w => 
      `👤 ${w.userId}\n` +
      `💰 ${w.amount} GB\n` +
      `📭 ${w.wallet}\n` +
      `📜 ${w.txHash}\n` +
      `⏱️ ${w.completedAt.toLocaleString()}\n` +
      `──────────────────`
    ).join('\n');
    
    await ctx.reply(`📜 <b>Recent Withdrawals</b>\n\n${text}`, {
      parse_mode: 'HTML',
      ...Markup.keyboard([
        ['🔙 Admin Menu']
      ]).resize()
    });
  } catch (e) {
    console.error('Recent withdrawals error:', e);
    await ctx.reply('❌ Error loading recent withdrawals. Please try again.', Markup.keyboard([
      ['🔙 Admin Menu']
    ]).resize());
  }
});

bot.hears('📜 Failed Withdrawals', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  
  try {
    const failedWithdrawals = await db.collection('withdrawals')
      .find({ status: 'failed' })
      .sort({ date: -1 })
      .limit(10)
      .toArray();
    
    if (failedWithdrawals.length === 0) {
      return ctx.reply('No failed withdrawals found.', Markup.keyboard([['🔙 Admin Menu']]).resize());
    }
    
    const text = failedWithdrawals.map((w, i) => {
      // Handle cases where date might be missing or invalid
      const dateDisplay = w.date 
        ? new Date(w.date).toLocaleString() 
        : 'Unknown date';
      
      return `${i + 1}. 👤 ${w.userId}\n` +
        `💰 ${w.amount} GB\n` +
        `📭 ${w.wallet || 'No wallet'}\n` +
        `❌ ${w.error || 'Unknown error'}\n` +
        `⏱️ ${dateDisplay}\n` +
        `──────────────────`;
    }).join('\n');
    
    await ctx.reply(`📜 <b>Failed Withdrawals</b>\n\n${text}`, {
      parse_mode: 'HTML',
      ...Markup.inlineKeyboard([
        [Markup.button.callback('🔄 Retry All', 'retry_failed')]
      ]),
      ...Markup.keyboard([['🔙 Admin Menu']]).resize()
    });
  } catch (e) {
    console.error('Failed withdrawals error:', e);
    await ctx.reply('❌ Error loading failed withdrawals.', Markup.keyboard([['🔙 Admin Menu']]).resize());
  }
});

bot.action('retry_failed', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  await ctx.answerCbQuery('Processing...');
  
  try {
    const failedWithdrawals = await db.collection('withdrawals')
      .find({ status: 'failed', retryCount: { $lt: config.MAX_RETRIES } })
      .toArray();
    
    if (failedWithdrawals.length === 0) {
      return ctx.reply('No failed withdrawals to retry.');
    }
    
    await ctx.reply(`⏳ Retrying ${failedWithdrawals.length} failed withdrawals...`);
    
    for (const withdrawal of failedWithdrawals) {
      try {
        await db.collection('withdrawals').updateOne(
          { _id: withdrawal._id },
          { $set: { status: 'pending', retryCount: (withdrawal.retryCount || 0) + 1 } }
        );
      } catch (e) {
        console.error(`Failed to retry withdrawal ${withdrawal._id}:`, e);
      }
    }
    
    await ctx.reply(`✅ ${failedWithdrawals.length} withdrawals queued for retry.`);
  } catch (e) {
    console.error('Retry failed withdrawals error:', e);
    await ctx.reply('❌ Error retrying withdrawals.');
  }
});

bot.hears('🛡️ Ban Tools', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  
  try {
    const stats = await getSystemStats();
    await ctx.reply(`🛡️ <b>Ban Management</b>\n\n` +
      `⛔ Banned Users: ${stats.bannedUsers}\n` +
      `⚠️ Recent Bans (24h): ${stats.recentBans}`, {
      parse_mode: 'HTML',
      ...Markup.keyboard([
        ['🔨 Ban User', '🔓 Unban User'],
        ['📜 Banned Users List', '🔙 Admin Menu']
      ]).resize()
    });
  } catch (e) {
    console.error('Ban tools error:', e);
    ctx.reply('❌ Error loading ban tools. Please try again.');
  }
});

bot.hears('🔨 Ban User', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  ctx.session.state = 'admin_ban';
  await ctx.reply('Enter user ID to ban:', Markup.keyboard([['🔙 Cancel']]).resize());
});

bot.hears('🔓 Unban User', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  ctx.session.state = 'admin_unban';
  await ctx.reply('Enter user ID to unban:', Markup.keyboard([['🔙 Cancel']]).resize());
});

bot.hears('📜 Banned Users List', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  
  try {
    const bannedUsers = await db.collection('users')
      .find({ isBanned: true })
      .sort({ banDate: -1 })
      .limit(50)
      .toArray();
    
    if (bannedUsers.length === 0) {
      return ctx.reply('No banned users found.', Markup.keyboard([['🔙 Admin Menu']]).resize());
    }
    
    const text = bannedUsers.map((user, index) => 
      `${index + 1}. 👤 ${user.userId}\n` +
      `⏱️ ${user.banDate?.toLocaleString() || 'Unknown'}\n` +
      `──────────────────`
    ).join('\n');
    
    await ctx.reply(`📜 <b>Banned Users</b>\n\n${text}`, {
      parse_mode: 'HTML',
      ...Markup.keyboard([['🔙 Admin Menu']]).resize()
    });
  } catch (e) {
    console.error('Banned users list error:', e);
    await ctx.reply('❌ Error loading banned users list.', Markup.keyboard([['🔙 Admin Menu']]).resize());
  }
});

bot.hears('📦 Bulk Actions', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  
  await ctx.reply(`📦 <b>Bulk Actions</b>\n\n` +
    `Perform actions on multiple users at once.`, {
    parse_mode: 'HTML',
    ...Markup.keyboard([
      ['🔨 Bulk Ban', '🔓 Bulk Unban'],
      ['➕ Bulk Add Tokens', '➖ Bulk Remove Tokens'],
      ['📩 Bulk Message', '🔙 Admin Menu']
    ]).resize()
  });
});

bot.hears('📩 Bulk Message', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  ctx.session.state = 'admin_bulk_message';
  await ctx.reply('Enter the message you want to send to multiple users:', Markup.keyboard([['🔙 Cancel']]).resize());
});

bot.hears('🔨 Bulk Ban', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  await ctx.scene.enter('bulkAction', { action: 'ban' });
});

bot.hears('🔓 Bulk Unban', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  await ctx.scene.enter('bulkAction', { action: 'unban' });
});

bot.hears('➕ Bulk Add Tokens', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  await ctx.scene.enter('bulkAction', { action: 'add_tokens' });
});

bot.hears('➖ Bulk Remove Tokens', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  await ctx.scene.enter('bulkAction', { action: 'remove_tokens' });
});

bot.hears('🔄 Refresh Stats', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  
  try {
    const stats = await getSystemStats();
    await ctx.reply(`🔄 <b>Stats Refreshed</b>\n\n` +
      `👥 Users: ${stats.totalUsers}\n` +
      `💰 Tokens: ${stats.totalTokens}\n` +
      `⏳ Active: ${stats.activeUsers}\n` +
      `⏳ Pending Withdrawals: ${stats.pendingWithdrawals}\n` +
      `✅ Completed Withdrawals: ${stats.completedWithdrawals}`, {
      parse_mode: 'HTML'
    });
  } catch (e) {
    console.error('Refresh stats error:', e);
    ctx.reply('❌ Error refreshing stats. Please try again.');
  }
});

bot.hears('🔙 Admin Menu', async (ctx) => {
  if (!isAdmin(ctx.from.id)) return;
  await showAdminPanel(ctx);
});

bot.hears('🏠 Main Menu', async (ctx) => {
  await showMainMenu(ctx);
});

// ==================== [WITHDRAWAL FLOW] ====================

bot.hears('💸 Withdraw', async ctx => {
  try {
    const user = await db.collection('users').findOne({ userId: ctx.from.id });
    if (!user) return ctx.reply('❌ You must join and verify first.');
    if (!user.wallet) return ctx.reply('❌ Please submit your wallet first.');

    const balance = await db.collection('balances').findOne({ userId: ctx.from.id });
    if (!balance || balance.tokens < config.MIN_WITHDRAW_AMOUNT) {
      return ctx.reply(`❌ Minimum withdrawal is ${config.MIN_WITHDRAW_AMOUNT} tokens. Your balance: ${balance?.tokens || 0}`);
    }

    ctx.session.withdrawalData = { step: 'awaiting_amount' };
    await ctx.reply(`💸 <b>Withdrawal Request</b>\n\nYour balance: ${balance.tokens} GB\n\nPlease enter amount to withdraw:`, {
      parse_mode: 'HTML',
      ...Markup.keyboard([['🔙 Cancel']]).resize()
    });
  } catch (e) {
    console.error('Withdrawal init error:', e);
    ctx.reply('❌ Error processing withdrawal. Please try again.');
  }
});

// ==================== [TEXT HANDLER] ====================

bot.on('text', async (ctx) => {
  if (!ctx.session) ctx.session = {};
  
  if (ctx.message.text === '🔙 Cancel') {
    ctx.session.state = null;
    ctx.session.adminAction = null;
    ctx.session.withdrawalData = null;
    ctx.session.bulkMessage = null;
    ctx.session.bulkMessageTarget = null;
    ctx.session.airdropData = null;
    return showMainMenu(ctx);
  }

  if (ctx.session.withdrawalData) {
    try {
      if (ctx.session.withdrawalData.step === 'awaiting_amount') {
        const amount = ctx.message.text.toLowerCase() === 'all' 
          ? (await db.collection('balances').findOne({ userId: ctx.from.id })).tokens
          : parseInt(ctx.message.text);
        
        if (isNaN(amount)) throw new Error('Please enter a valid number');
        if (amount < config.MIN_WITHDRAW_AMOUNT) throw new Error(`Minimum withdrawal is ${config.MIN_WITHDRAW_AMOUNT} tokens`);
        
        const balance = await db.collection('balances').findOne({ userId: ctx.from.id });
        if (!balance || amount > balance.tokens) throw new Error('Insufficient balance');
        
        ctx.session.withdrawalData = {
          ...ctx.session.withdrawalData,
          step: 'awaiting_confirmation',
          amount,
          currentBalance: balance.tokens
        };
        
        const user = await db.collection('users').findOne({ userId: ctx.from.id });
        return ctx.reply(`🔐 <b>Withdrawal Confirmation</b>\n\n` +
          `Amount: ${amount} GB\n` +
          `Wallet: ${user.wallet}\n` +
          `New Balance: ${balance.tokens - amount} GB\n\n` +
          `Are you sure?`, {
          parse_mode: 'HTML',
          ...Markup.keyboard([
            ['✅ Confirm Withdrawal'],
            ['✏️ Change Amount', '🔙 Cancel']
          ]).resize()
        });
      }
      
      if (ctx.session.withdrawalData.step === 'awaiting_confirmation') {
        if (ctx.message.text === '✅ Confirm Withdrawal') {
          const { amount } = ctx.session.withdrawalData;
          const user = await db.collection('users').findOne({ userId: ctx.from.id });
          
          await db.collection('balances').updateOne(
            { userId: ctx.from.id },
            { $inc: { tokens: -amount } }
          );
          
          await db.collection('withdrawals').insertOne({
            userId: ctx.from.id,
            amount,
            wallet: user.wallet,
            status: 'pending',
            date: new Date()
          });
          
          await ctx.reply(`🎉 Withdrawal request for ${amount} GB submitted successfully!\n\nIt will be processed automatically within the next 5 minutes.`);
          
          try {
            await bot.telegram.sendMessage(
              config.PAYOUT_CHANNEL,
              `💸 New Auto-Withdrawal Request\n\n` +
              `👤 User: ${ctx.from.id}\n` +
              `💰 Amount: ${amount} GB\n` +
              `📭 Wallet: ${user.wallet}\n\n` +
              `⏳ Will be processed automatically`
            );
          } catch (e) {
            console.error('Failed to notify payout channel:', e);
          }
          
          ctx.session.withdrawalData = null;
          return showMainMenu(ctx);
        }
        
        if (ctx.message.text === '✏️ Change Amount') {
          ctx.session.withdrawalData.step = 'awaiting_amount';
          return ctx.reply('Please enter the new amount to withdraw:', {
            ...Markup.keyboard([['🔙 Cancel']]).resize()
          });
        }
      }
    } catch (error) {
      await ctx.reply(`❌ ${error.message}`);
      ctx.session.withdrawalData = null;
      return showMainMenu(ctx);
    }
    return;
  }

  // Handle Find User
  if (ctx.session.state === 'admin_find_user') {
    try {
      const userId = parseInt(ctx.message.text);
      if (isNaN(userId)) throw new Error('Invalid user ID');
      
      const { user, balance, referrals } = await getUserDetails(userId);
      
      if (!user) throw new Error('User not found');
      
      let message = `👤 <b>User Details</b>\n\n` +
        `ID: ${userId}\n` +
        `Status: ${user.isBanned ? '⛔ Banned' : '🟢 Active'}\n` +
        `Wallet: ${user.wallet || 'Not set'}\n` +
        `Twitter: ${user.twitter || 'Not set'}\n` +
        `Referrals: ${referrals}\n` +
        `Balance: ${balance?.tokens || 0} GB`;
      
      const keyboard = [
        [user.isBanned ? '🔓 Unban User' : '🔨 Ban User'],
        ['➕ Add Tokens', '➖ Remove Tokens'],
        ['📩 Send Message', '🔙 Admin Menu']
      ];
      
      await ctx.replyWithHTML(message, Markup.keyboard(keyboard).resize());
      
      ctx.session.state = null;
      ctx.session.currentUser = userId;
    } catch (error) {
      await ctx.reply(`❌ ${error.message}`);
      ctx.session.state = null;
    }
    return;
  }

  // Handle Bulk Message
  if (ctx.session.state === 'admin_bulk_message') {
    ctx.session.bulkMessage = ctx.message.text;
    await ctx.reply(`Who should receive this message?`, {
      ...Markup.keyboard([
        ['👥 All Users', '🟢 Active Users'],
        ['📋 Specific Users', '🔙 Cancel']
      ]).resize()
    });
    ctx.session.state = 'admin_bulk_message_target';
    return;
  }

  if (ctx.session.state === 'admin_bulk_message_target') {
    try {
      if (ctx.message.text === '👥 All Users') {
        const count = await db.collection('users').countDocuments();
        ctx.session.bulkMessageTarget = { type: 'all', count };
      } 
      else if (ctx.message.text === '🟢 Active Users') {
        const count = await db.collection('users').countDocuments({ 
          lastActive: { $gt: new Date(Date.now() - 86400000) } 
        });
        ctx.session.bulkMessageTarget = { type: 'active', count };
      }
      else if (ctx.message.text === '📋 Specific Users') {
        ctx.session.state = 'admin_bulk_message_specific';
        return ctx.reply('Send user IDs separated by commas:', 
          Markup.keyboard([['🔙 Cancel']]).resize());
      }
      else {
        throw new Error('Invalid selection');
      }
      
      await ctx.replyWithHTML(`⚠️ <b>Confirm Bulk Message</b>\n\n` +
        `Recipients: ${ctx.session.bulkMessageTarget.type} (${ctx.session.bulkMessageTarget.count} users)\n\n` +
        `Message: ${ctx.session.bulkMessage}\n\n` +
        `Send this message?`, {
        ...Markup.keyboard([
          ['✅ Send Message'],
          ['✏️ Edit Message', '🔙 Cancel']
        ]).resize()
      });
      
      ctx.session.state = 'admin_bulk_message_confirm';
    } catch (error) {
      await ctx.reply(`❌ ${error.message}`);
      ctx.session.state = null;
      ctx.session.bulkMessage = null;
      ctx.session.bulkMessageTarget = null;
    }
    return;
  }

  if (ctx.session.state === 'admin_bulk_message_specific') {
    try {
      const users = ctx.message.text.split(',').map(id => parseInt(id.trim())).filter(id => !isNaN(id));
      
      if (users.length === 0) throw new Error('No valid user IDs provided');
      if (users.length > config.MAX_BULK_ACTIONS) {
        throw new Error(`Maximum ${config.MAX_BULK_ACTIONS} users allowed in bulk message`);
      }
      
      ctx.session.bulkMessageTarget = { 
        type: 'specific', 
        users,
        count: users.length
      };
      
      await ctx.replyWithHTML(`⚠️ <b>Confirm Bulk Message</b>\n\n` +
        `Recipients: ${users.length} specific users\n\n` +
        `Message: ${ctx.session.bulkMessage}\n\n` +
        `Send this message?`, {
        ...Markup.keyboard([
          ['✅ Send Message'],
          ['✏️ Edit Message', '🔙 Cancel']
        ]).resize()
      });
      
      ctx.session.state = 'admin_bulk_message_confirm';
    } catch (error) {
      await ctx.reply(`❌ ${error.message}`);
      ctx.session.state = null;
      ctx.session.bulkMessage = null;
      ctx.session.bulkMessageTarget = null;
    }
    return;
  }

  if (ctx.session.state === 'admin_bulk_message_confirm' && ctx.message.text === '✅ Send Message') {
    try {
      const { type, count, users } = ctx.session.bulkMessageTarget;
      let userList = [];
      
      if (type === 'all') {
        userList = await db.collection('users').find().project({ userId: 1 }).toArray();
      } 
      else if (type === 'active') {
        userList = await db.collection('users').find({ 
          lastActive: { $gt: new Date(Date.now() - 86400000) } 
        }).project({ userId: 1 }).toArray();
      }
      else if (type === 'specific') {
        userList = users.map(userId => ({ userId }));
      }
      
      await ctx.reply(`⏳ Sending message to ${userList.length} users...`);
      
      let successCount = 0;
      let failCount = 0;
      
      for (const user of userList) {
        try {
          await bot.telegram.sendMessage(
            user.userId,
            ctx.session.bulkMessage,
            { parse_mode: 'Markdown' }
          );
          successCount++;
          
          // Rate limiting
          if (successCount % 20 === 0) {
            await new Promise(resolve => setTimeout(resolve, 1000));
          }
        } catch (e) {
          failCount++;
          console.error(`Could not send to user ${user.userId}:`, e);
        }
      }
      
      await ctx.replyWithHTML(`✅ <b>Bulk Message Completed</b>\n\n` +
        `Total recipients: ${userList.length}\n` +
        `Successfully sent: ${successCount}\n` +
        `Failed to send: ${failCount}`);
      
      ctx.session.bulkMessage = null;
      ctx.session.bulkMessageTarget = null;
      ctx.session.state = null;
    } catch (error) {
      console.error('Bulk message error:', error);
      await ctx.reply(`❌ Bulk message failed: ${error.message}`);
      ctx.session.bulkMessage = null;
      ctx.session.bulkMessageTarget = null;
      ctx.session.state = null;
    }
    return;
  }

  // Handle Airdrop to All
  if (ctx.session.state === 'admin_airdrop_all') {
    try {
      const amount = parseInt(ctx.message.text);
      if (isNaN(amount) || amount <= 0) throw new Error('Invalid amount');
      
      const userCount = await db.collection('users').countDocuments();
      
      ctx.session.airdropData = {
        type: 'all',
        amount,
        userCount
      };
      
      await confirmAirdrop(ctx);
    } catch (error) {
      await ctx.reply(`❌ ${error.message}`);
      ctx.session.state = null;
      ctx.session.airdropData = null;
    }
    return;
  }

  // Handle Airdrop to Active
  if (ctx.session.state === 'admin_airdrop_active') {
    try {
      const amount = parseInt(ctx.message.text);
      if (isNaN(amount) || amount <= 0) throw new Error('Invalid amount');
      
      const activeUsers = await db.collection('users').countDocuments({ 
        lastActive: { $gt: new Date(Date.now() - 86400000) } 
      });
      
      ctx.session.airdropData = {
        type: 'active',
        amount,
        userCount: activeUsers
      };
      
      await confirmAirdrop(ctx);
    } catch (error) {
      await ctx.reply(`❌ ${error.message}`);
      ctx.session.state = null;
      ctx.session.airdropData = null;
    }
    return;
  }

  // Handle Airdrop to List
  if (ctx.session.state === 'admin_airdrop_list') {
    try {
      const users = ctx.message.text.split(',').map(id => parseInt(id.trim())).filter(id => !isNaN(id));
      
      if (users.length === 0) throw new Error('No valid user IDs provided');
      if (users.length > config.MAX_BULK_ACTIONS) {
        throw new Error(`Maximum ${config.MAX_BULK_ACTIONS} users allowed in airdrop`);
      }
      
      ctx.session.airdropData = {
        type: 'list',
        users,
        userCount: users.length
      };
      
      await ctx.reply('Enter amount to airdrop to each user:', 
        Markup.keyboard([['🔙 Cancel']]).resize());
      
      ctx.session.state = 'admin_airdrop_list_amount';
    } catch (error) {
      await ctx.reply(`❌ ${error.message}`);
      ctx.session.state = null;
      ctx.session.airdropData = null;
    }
    return;
  }

  if (ctx.session.state === 'admin_airdrop_list_amount') {
    try {
      const amount = parseInt(ctx.message.text);
      if (isNaN(amount) || amount <= 0) throw new Error('Invalid amount');
      
      ctx.session.airdropData.amount = amount;
      await confirmAirdrop(ctx);
    } catch (error) {
      await ctx.reply(`❌ ${error.message}`);
      ctx.session.state = null;
      ctx.session.airdropData = null;
    }
    return;
  }

  if (ctx.session.airdropData && ctx.message.text === '✅ Confirm Airdrop') {
    try {
      const { type, amount, users, userCount } = ctx.session.airdropData;
      let userList = [];
      
      if (type === 'all') {
        userList = await db.collection('users').find().project({ userId: 1 }).toArray();
      } 
      else if (type === 'active') {
        userList = await db.collection('users').find({ 
          lastActive: { $gt: new Date(Date.now() - 86400000) } 
        }).project({ userId: 1 }).toArray();
      }
      else if (type === 'list') {
        userList = users.map(userId => ({ userId }));
      }
      
      await ctx.reply(`⏳ Airdropping ${amount} GB to ${userList.length} users...`);
      
      const bulkOps = userList.map(user => ({
        updateOne: {
          filter: { userId: user.userId },
          update: { $inc: { tokens: amount } },
          upsert: true
        }
      }));
      
      // Split into chunks to avoid hitting MongoDB limits
      const chunkSize = 500;
      for (let i = 0; i < bulkOps.length; i += chunkSize) {
        const chunk = bulkOps.slice(i, i + chunkSize);
        await db.collection('balances').bulkWrite(chunk);
      }
      
      // Notify users
      let notifiedCount = 0;
      for (const user of userList) {
        try {
          await bot.telegram.sendMessage(
            user.userId,
            `🎁 You received an airdrop of ${amount} GB tokens!\n\n` +
            `Your balance has been updated.`
          );
          notifiedCount++;
        } catch (e) {
          console.error(`Could not notify user ${user.userId}:`, e);
        }
      }
      
      await ctx.replyWithHTML(`✅ <b>Airdrop Completed</b>\n\n` +
        `Sent ${amount} GB to ${userList.length} users\n` +
        `Successfully notified: ${notifiedCount}\n` +
        `Total tokens distributed: ${amount * userList.length} GB`);
      
      ctx.session.airdropData = null;
      ctx.session.state = null;
    } catch (error) {
      console.error('Airdrop error:', error);
      await ctx.reply(`❌ Airdrop failed: ${error.message}`);
      ctx.session.airdropData = null;
      ctx.session.state = null;
    }
    return;
  }

  if (ctx.session.state && ctx.session.state.startsWith('admin_')) {
    try {
      const action = ctx.session.state.split('_')[1];
      const parts = ctx.message.text.split(' ');
      const userId = parseInt(parts[0]);
      const amount = parts[1] ? parseInt(parts[1]) : 0;
      
      if (action === 'ban' || action === 'unban') {
        await db.collection('users').updateOne(
          { userId },
          { $set: { 
            isBanned: action === 'ban',
            banDate: action === 'ban' ? new Date() : null
          } }
        );
        await ctx.reply(`✅ User ${userId} ${action === 'ban' ? 'banned' : 'unbanned'} successfully!`);
      }
      else if (action === 'add' || action === 'remove') {
        if (isNaN(amount) || amount <= 0) {
          throw new Error('Please enter a valid positive amount');
        }
        await db.collection('balances').updateOne(
          { userId },
          { $inc: { tokens: action === 'add' ? amount : -amount } },
          { upsert: true }
        );
        await ctx.reply(`✅ ${action === 'add' ? 'Added' : 'Removed'} ${amount} tokens ${action === 'add' ? 'to' : 'from'} user ${userId}`);
      }
      
      ctx.session.state = null;
      return showMainMenu(ctx);
    } catch (error) {
      await ctx.reply(`❌ Error: ${error.message}`);
      ctx.session.state = null;
      return showMainMenu(ctx);
    }
  }

  if (ctx.session.state === 'awaiting_twitter') {
    if (ctx.message.text.startsWith('@')) {
      await db.collection('users').updateOne(
        { userId: ctx.from.id },
        { $set: { twitter: ctx.message.text } },
        { upsert: true }
      );
      ctx.session.state = 'awaiting_wallet';
      return ctx.reply('🔐 Please send your Polygon wallet address (starts with 0x)', Markup.keyboard([['🔙 Cancel']]).resize());
    }
    return ctx.reply('❌ Please send a valid Twitter username starting with @');
  }

  if (ctx.session.state === 'awaiting_wallet') {
    if (ctx.message.text.startsWith('0x')) {
      await db.collection('users').updateOne(
        { userId: ctx.from.id },
        { $set: { wallet: ctx.message.text } },
        { upsert: true }
      );
      ctx.session.state = null;
      return showMainMenu(ctx);
    }
    return ctx.reply('❌ Please send a valid Polygon wallet address starting with 0x');
  }

  ctx.reply('❓ I did not understand that. Please use the menu buttons.');
});

// ==================== [VERIFICATION ACTIONS] ====================

bot.action('verify_join', async ctx => {
  await ctx.answerCbQuery();
  const msg = `🔘 Join our [Channel](${config.LINKS.channel}) & [Group](${config.LINKS.group})
✅ Click "Done" after you finish.`;
  const sent = await ctx.replyWithMarkdown(msg, Markup.inlineKeyboard([
    [Markup.button.callback('✅ Done', 'verify_done')],
    [Markup.button.callback('🚫 Cancel', 'verify_cancel')]
  ]));
  ctx.session.verifyMsgId = sent.message_id;
});

bot.action('verify_done', async ctx => {
  await ctx.answerCbQuery();
  try {
    if (ctx.session.verifyMsgId) {
      await ctx.deleteMessage(ctx.session.verifyMsgId);
    }
    
    for (let group of config.REQUIRED_GROUPS) {
      try {
        const status = await ctx.telegram.getChatMember(group, ctx.from.id);
        if (["left", "kicked"].includes(status.status)) {
          return ctx.reply(`❌ You must join ${group} first.`);
        }
      } catch (e) {
        console.error(`Error checking group membership for ${group}:`, e);
        return ctx.reply(`❌ Could not verify your membership in ${group}. Please try again.`);
      }
    }
    
    ctx.session.state = 'awaiting_twitter';
    await ctx.replyWithMarkdown(`🔘 Follow our [Twitter](${config.LINKS.twitter})
💬 Send your Twitter username starting with @`, Markup.keyboard([['🔙 Cancel']]).resize());
  } catch (e) {
    console.error('Verification error:', e);
    ctx.reply('⚠️ Could not verify your join. Try again.');
  }
});

bot.action('verify_cancel', async ctx => {
  await ctx.answerCbQuery();
  ctx.reply('❌ Verification cancelled. Use /start to try again.');
  if (ctx.session.verifyMsgId) {
    try {
      await ctx.deleteMessage(ctx.session.verifyMsgId);
    } catch (e) {
      console.log('Could not delete verify message');
    }
  }
});

// ==================== [HELPER FUNCTIONS] ====================

async function confirmAirdrop(ctx) {
  const { type, amount, userCount } = ctx.session.airdropData;
  
  await ctx.replyWithHTML(`⚠️ <b>Airdrop Confirmation</b>\n\n` +
    `Type: ${type === 'all' ? 'All users' : type === 'active' ? 'Active users' : 'Specific users'}\n` +
    `Amount: ${amount} GB each\n` +
    `Total recipients: ${userCount}\n` +
    `Total tokens: ${amount * userCount} GB\n\n` +
    `Are you sure?`, {
    ...Markup.keyboard([
      ['✅ Confirm Airdrop'],
      ['✏️ Change Amount', '🔙 Cancel']
    ]).resize()
  });
}

async function getUserDetails(userId) {
  const [user, balance, referrals] = await Promise.all([
    db.collection('users').findOne({ userId }),
    db.collection('balances').findOne({ userId }),
    db.collection('users').countDocuments({ referrerId: userId })
  ]);
  
  return { user, balance, referrals };
}

// ==================== [LAUNCH] ====================

async function launch() {
  try {
    await mongoClient.connect();
    db = mongoClient.db('GigabyteAirdropDB');
    
    // Create indexes
    await db.collection('users').createIndex({ userId: 1 });
    await db.collection('balances').createIndex({ userId: 1 });
    await db.collection('withdrawals').createIndex({ userId: 1 });
    await db.collection('withdrawals').createIndex({ status: 1 });
    await db.collection('withdrawals').createIndex({ date: 1 });
    await db.collection('withdrawals').createIndex({ completedAt: -1 });
    await db.collection('users').createIndex({ isBanned: 1 });
    await db.collection('users').createIndex({ banDate: -1 });
    
    // Start the bot
    bot.launch();
    console.log('🚀 Bot is running with all features...');
    
    // Start the withdrawal processing loop
    processWithdrawals().catch(err => {
      console.error('Error in initial withdrawal processing:', err);
    });
    
    // Notify admins
    for (const id of config.ADMIN_IDS) {
      try {
        await bot.telegram.sendMessage(id, '🤖 Bot is online with all features!', {
          reply_markup: {
            keyboard: [['🛠️ Admin Panel']],
            resize_keyboard: true
          }
        });
      } catch (e) {
        console.log(`Could not notify admin ${id}`);
      }
    }
  } catch (error) {
    console.error('❌ Failed to connect to MongoDB:', error);
    process.exit(1);
  }
}

process.once('SIGINT', () => {
  console.log('Shutting down gracefully...');
  bot.stop('SIGINT');
  mongoClient.close().then(() => process.exit(0));
});

process.once('SIGTERM', () => {
  console.log('Shutting down gracefully...');
  bot.stop('SIGTERM');
  mongoClient.close().then(() => process.exit(0));
});

launch();