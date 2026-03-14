import { instanceToPlain } from 'class-transformer';
import { AfterLoad } from 'typeorm';

/**
 * EntityRelationalHelper —— 所有 TypeORM 实体的公共基类
 *
 * 提供两个通用能力，所有继承它的 Entity（如 BlockEntity、TransactionEntity）
 * 都会自动获得这两个能力，无需重复实现：
 *   1. 实体加载后自动记录自身类名（__entity）
 *   2. 序列化为 JSON 时自动应用 class-transformer 的转换规则
 */
export class EntityRelationalHelper {
  /**
   * 实体类型标识字段
   *
   * 不映射到数据库列（无 @Column 装饰器），仅存在于内存中的运行时属性。
   * 值为当前实体的类名，例如 BlockEntity 实例的 __entity 值为 "BlockEntity"。
   * 用途：在多态场景下（如联合查询返回多种实体混合结果时）区分对象类型。
   */
  __entity?: string;

  /**
   * @AfterLoad —— TypeORM 生命周期钩子
   * 每当 TypeORM 从数据库查询并加载实体实例后，自动调用此方法。
   * 将当前实体的构造函数名赋值给 __entity，实现运行时类型标注。
   *
   * 例：this.constructor.name 对 BlockEntity 实例返回 "BlockEntity"
   */
  @AfterLoad()
  setEntityName() {
    this.__entity = this.constructor.name;
  }

  /**
   * toJSON —— 自定义 JSON 序列化行为
   *
   * 覆盖 JS 原生的 toJSON()，改用 class-transformer 的 instanceToPlain()。
   * 作用：当实体对象被 JSON.stringify() 或 NestJS 的响应序列化时，
   * 会自动应用 @Exclude、@Expose、@Transform 等装饰器的转换规则。
   *
   * 例：某字段标注了 @Exclude()，序列化时该字段会被自动过滤，不暴露给客户端。
   */
  toJSON() {
    return instanceToPlain(this);
  }
}
