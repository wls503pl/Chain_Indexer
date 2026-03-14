/**
 * BlockMeta —— Kafka 消息中"区块元信息"的数据结构
 * 对应 Solana 链上每个区块的基础属性，由 Yellowstone gRPC 推送后序列化(Serialization)传入 Kafka
 */
export interface BlockMeta {
  slot: bigint; // 当前区块的 slot 编号（Solana 以 slot 而非 blockNumber 标识区块位置）
  blockhash: string; // 当前区块的哈希值，用于唯一标识这个区块
  rewards: {
    rewards: any[]; // 区块奖励列表（含验证者奖励、租金等），结构复杂暂用 any[]
  };
  block_time: {
    timestamp: number; // 区块生成的 Unix 时间戳（秒级），由 leader 节点记录
  };
  block_height: {
    block_height: bigint; // 区块高度，从创世块算起已确认的区块总数
  };
  parent_slot: bigint; // 父区块的 slot 编号，用于构建链式结构 / 检测 reorg
  parent_blockhash: string; // 父区块的哈希，与 parent_slot 配合做链完整性校验
  executed_transaction_count: number; // 本 slot 中实际执行成功的交易数量
  entries_count: number; // 本 slot 中的 entry 数量（entry 是 Solana PoH 的基本单元，包含多笔交易）
}

/**
 * Account —— Kafka 消息中"账户状态变更"的数据结构
 * 每当链上账户数据发生变化（余额/数据更新），Yellowstone gRPC 会推送该结构
 * Solana 的账户模型：所有状态都存在账户里，程序(Program)本身也是一种账户
 */
export interface Account {
  pubkey: string; // 账户的公钥地址（Base58 编码），相当于账户的唯一 ID
  lamports: number; // 账户余额，单位是 lamport（1 SOL = 10^9 lamports）
  owner: any; // 该账户归属的程序 ID（即哪个程序"拥有"并可写入此账户）
  executable: boolean; // 是否为可执行账户（true = 这是一个部署好的程序账户）
  rent_epoch: number; // 下次需要缴纳租金的 epoch 编号（账户需维持最低余额否则被清除）
  data: any; // 账户存储的原始业务数据（不同程序有不同的序列化格式，如 Borsh）
  write_version: number; // 账户数据的写入版本号，用于判断数据新旧 / 去重
  txn_signature: any; // 导致本次账户状态变更的交易签名（可为 null，如区块奖励触发的变更）
}

/**
 * Transaction —— Kafka 消息中"完整交易"的数据结构
 * 包含交易本体 + 执行结果 meta，是消费者(Kafka Consumer)处理业务逻辑的核心对象
 */
export interface Transaction {
  signature: string; // 交易签名（Base58 编码），是交易的唯一标识符
  is_vote: boolean; // 是否为投票交易（Solana 共识机制中验证者的投票，占交易量大头，通常过滤掉）
  transaction: {
    signatures: any; // 所有签名方的签名列表（多签场景下会有多个）
    message: {
      header: Header; // 消息头，描述签名账户数量等权限信息（见 Header 接口）
      account_keys: any; // 本交易涉及的所有账户公钥列表
      recent_blockhash: number[]; // 最近区块哈希（字节数组形式），防止重放攻击 / 设置交易有效期
      instructions: Instruction[]; // 指令列表，每条指令调用一个链上程序（见 Instruction 接口）
      versioned: boolean; // 是否为版本化交易（v0 支持 Address Lookup Table，节省空间）
      address_table_lookups: any[]; // 地址查找表引用列表（versioned tx 专属，用于压缩账户地址）
    };
  };
  slot: number; // 该交易所在的 slot 编号
  meta: Meta; // 交易执行结果元数据（费用、余额变化、日志等，见 Meta 接口）
  index: number; // 该交易在当前 slot 中的排列索引（用于排序和幂等处理）
}

/**
 * Meta —— 交易执行结果的元数据
 * 链上执行完成后由节点填充，包含费用、余额快照、日志等调试/分析关键信息
 */
export interface Meta {
  err: null; // 执行错误信息（null 表示成功；失败时为错误对象）
  fee: number; // 本笔交易消耗的手续费（单位 lamport）
  pre_balances: number[]; // 交易执行前各账户的 SOL 余额快照（与 account_keys 顺序对应）
  post_balances: number[]; // 交易执行后各账户的 SOL 余额快照（用于计算余额变动）
  inner_instructions: any[]; // 内部指令列表（CPI 跨程序调用产生的子指令）
  inner_instructions_none: boolean; // 是否没有内部指令（true = inner_instructions 为空，优化标志）
  log_messages: string[]; // 程序执行日志（Program.log() 输出，调试/事件解析的重要来源）
  log_messages_none: boolean; // 是否没有日志（true = log_messages 为空）

  /**
   * SPL: Solana Program Library 相当于ETH 的ERC20 标准
   * 它是 Solana 官方维护的一组链上程序（类似"官方插件库"）。其中最核心的一个程序就是 Token Program,
   * 它规定了在 Solana 上创建、转账、销毁代币的统一逻辑:
   * 所有代币共享官方的 Token Program 程序，后者为每个代币单独开辟一个 ATA (Associated Token Account) 账户.
   */
  pre_token_balances: any[]; // 交易前 SPL Token 余额快照（用于追踪 Token 转账）
  post_token_balances: any[]; // 交易后 SPL Token 余额快照

  rewards: any[]; // 本交易产生的奖励（通常为空）
  loaded_writable_addresses: any[]; // 从地址查找表加载的可写地址（versioned tx 专属）
  loaded_readonly_addresses: any[]; // 从地址查找表加载的只读地址（versioned tx 专属）
  return_data: null; // 程序返回数据（set_return_data() 写入的值，可为 null）
  return_data_none: boolean; // 是否没有返回数据
  compute_units_consumed: number; // 本交易实际消耗的 Compute Units（用于性能分析和费用优化）
}

/**
 * Header —— 交易消息头
 * 描述交易中账户的权限分布，Solana runtime 用它来验证签名完整性
 */
export interface Header {
  num_required_signatures: number; // 必须提供签名的账户数量（account_keys 前 N 个账户需签名）
  num_readonly_signed_accounts: number; // 在签名账户中，只读账户的数量（不能被写入）
  num_readonly_unsigned_accounts: number; // 在未签名账户中，只读账户的数量
}

/**
 * Instruction —— 单条链上指令
 * 交易的最小执行单元，每条指令指定调用哪个程序、传入哪些账户、携带什么数据
 */
export interface Instruction {
  program_id_index: number; // 被调用程序在 account_keys 中的索引（而非直接存公钥，节省空间）
  accounts: number[]; // 该指令涉及的账户索引列表（同样是 account_keys 的下标）

  /**
   * Borsh 是 Binary Object Representation Serializer for Hashing 的缩写。
   * 它是一种专门为加密货币和分布式系统（特别是 Solana 和 Near 区块链）设计的二进制序列化格式
   * Borsh 序列化： 它只存储 5Alice25（示意）。它假设你已经提前知道：前 4 个字节存字符串长度，接着是字符串内容，最后是整数
   */
  data: number[]; // 指令数据（字节数组，程序自定义编码，通常是 Borsh 序列化的参数）
}

/**
 * Slot —— Kafka 消息中"slot 状态"的数据结构
 * 原项目从 @prisma/client 导入此类型，我们在此自定义以移除 Prisma 依赖。
 * Solana 的 slot 有三个确认阶段：
 *   processed（已处理）→ confirmed（已确认）→ finalized（已最终确认）
 */
export interface Slot {
  slot: number; // slot 编号
  parent: number; // 父 slot 编号
  status: string; // 当前确认状态：'processed' | 'confirmed' | 'finalized'
}
