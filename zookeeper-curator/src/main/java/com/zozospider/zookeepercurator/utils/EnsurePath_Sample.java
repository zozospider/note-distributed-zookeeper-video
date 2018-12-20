package com.zozospider.zookeepercurator.utils;

/**
 * EnsurePath 提供了一种能够确保数据节点存在的机制。
 * <p>
 * 多用于这样的业务场景中：
 * 上层业务希望对一个数据节点进行一些操作，但是操作之前需要确保该节点存在。基于 ZooKeeper 提供的原始 API 接口，
 * 为解决上述场景问题，开发人员需要首先对该节点进行一个判断，如果该节点不存在，那么就需要创建节点。
 * 而与此同时，在分布式环境中，在 A 机器试图进行节点的创建过程中，由于并发操作的存在，另一台机器，如 B 机器，也在同时创建这个节点，
 * 于是 A 机器创建的时候，可能会抛出诸如 "节点已经存在" 的异常，因此开发人员还必须对这些异常进行单独的处理，逻辑通常非常琐碎。
 * <p>
 * EnsurePath 正好可以用来解决这些烦人的问题，它采取了静默的节点创建方式，
 * 其内部实现就是试图创建指定节点，如果节点已经存在，那么就不进行任何操作，也不对外抛出异常，否则正常创建数据节点。
 */
public class EnsurePath_Sample {

    // TODO

}
