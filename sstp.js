import { connect } from 'cloudflare:sockets';

const 空字节 = new Uint8Array(0);
const 编码器 = new TextEncoder();
const 解码器 = new TextDecoder();
const SSTP连接超时 = 10_000;
const 最大早期数据字节 = 8 * 1024;
const 最大首包字节 = 64 * 1024;
const TCP最大分段 = 1400;
const 节点列表最大字节 = 256 * 1024;
const 节点列表缓存时长 = 5 * 60 * 1000;
const 节点连接尝试上限 = 5;

let 缓存节点列表 = null;
let 缓存节点到期 = 0;
let 刷新节点列表任务 = null;

export default {
	async fetch(请求, 环境) {
		if ((请求.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') {
			return new Response('VLESS over WebSocket with SSTP outbound\n', {
				headers: { 'content-type': 'text/plain; charset=utf-8' }
			});
		}

		try {
			const uuid = 环境.UUID || 环境.uuid;
			const uuid字节 = 解析UUID(uuid);
			const sstp代理 = 解析SSTP代理(请求.url, 环境);
			return await 处理VLESSWebSocket(请求, uuid字节, sstp代理);
		} catch (错误) {
			console.error('[SSTP Worker] 请求初始化失败:', 错误);
			return new Response('Worker configuration error', { status: 500 });
		}
	}
};

function 解析UUID(值) {
	if (typeof 值 !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(值)) {
		throw new Error('请设置有效的 UUID 环境变量');
	}
	return Uint8Array.from(值.replaceAll('-', '').match(/.{2}/g), 字节 => Number.parseInt(字节, 16));
}

function 解析SSTP代理(请求网址, 环境) {
	const 配置 = 环境.SSTP_PROXY || 环境.SSTP;
	const 节点列表地址 = 环境.SSTP_NODES_URL;
	const 路径地址 = new URL(请求网址).pathname.match(/\/sstp:\/\/([^/?#\s]+)/i)?.[1];
	const 地址 = 配置 || (!节点列表地址 && 路径地址 ? `sstp://${路径地址}` : '');
	if (!配置 && 节点列表地址) {
		let 列表网址;
		try {
			列表网址 = new URL(节点列表地址);
		} catch {
			throw new Error('SSTP_NODES_URL 必须是 HTTPS 文件地址');
		}
		if (列表网址.protocol !== 'https:') throw new Error('SSTP_NODES_URL 必须使用 HTTPS');
		return {
			nodesUrl: 列表网址.href,
			username: 环境.SSTP_USERNAME || 'vpn',
			password: 环境.SSTP_PASSWORD || 'vpn'
		};
	}
	if (!地址) throw new Error('请设置 SSTP_NODES_URL、SSTP_PROXY，或在路径中提供 /sstp://主机:端口');

	let 代理网址;
	try {
		代理网址 = new URL(地址);
	} catch {
		throw new Error('SSTP_PROXY 必须是 sstp:// 用户信息@主机:端口 格式');
	}
	if (代理网址.protocol.toLowerCase() !== 'sstp:' || !代理网址.hostname) {
		throw new Error('SSTP_PROXY 必须是 sstp:// 用户信息@主机:端口 格式');
	}

	const 端口 = Number(代理网址.port || 443);
	if (!Number.isInteger(端口) || 端口 < 1 || 端口 > 65535) throw new Error('SSTP 端口无效');
	return {
		hostname: 代理网址.hostname.replace(/^\[|\]$/g, ''),
		port: 端口,
		username: 代理网址.username ? decodeURIComponent(代理网址.username) : null,
		password: 代理网址.password ? decodeURIComponent(代理网址.password) : null
	};
}

async function 处理VLESSWebSocket(请求, uuid字节, sstp代理) {
	const [客户端, 服务端] = Object.values(new WebSocketPair());
	try {
		服务端.accept();
		服务端.binaryType = 'arraybuffer';
	} catch (错误) {
		console.error('[SSTP Worker] WebSocket 接受失败:', 错误);
		try { 服务端.close(); } catch { }
		return new Response('WebSocket upgrade failed', { status: 500 });
	}

	let 已关闭 = false;
	let 目标连接 = null;
	let 目标写入器 = null;
	let 首包缓存 = 空字节;
	let 处理队列 = Promise.resolve();

	const 关闭 = (代码 = 1000, 原因 = 'closed') => {
		if (已关闭) return;
		已关闭 = true;
		try { 目标写入器?.releaseLock(); } catch { }
		try { 目标连接?.close(); } catch { }
		try { 服务端.close(代码, 原因); } catch { }
	};

	const 发送 = 数据 => {
		if (已关闭) return;
		try {
			服务端.send(数据);
		} catch (错误) {
			console.error('[SSTP Worker] WebSocket 发送失败:', 错误);
			关闭(1011, 'send failed');
		}
	};

	const 报错并关闭 = 错误 => {
		if (错误) console.error('[SSTP Worker] VLESS/SSTP 连接失败:', 错误);
		关闭(1011, 'upstream failed');
	};

	const 转发下行 = async () => {
		const 读取器 = 目标连接.readable.getReader();
		try {
			for (;;) {
				const { value, done } = await 读取器.read();
				if (done) break;
				if (value?.byteLength) 发送(value);
				if (已关闭) break;
			}
		} catch (错误) {
			if (!已关闭) 报错并关闭(错误);
		} finally {
			try { 读取器.releaseLock(); } catch { }
			if (!已关闭) 关闭();
		}
	};

	const 处理数据 = async 数据 => {
		if (已关闭) return;
		const 字节 = await 转换为字节(数据);
		if (!字节.byteLength) return;

		if (目标写入器) {
			await 目标写入器.write(字节);
			return;
		}

		if (首包缓存.byteLength + 字节.byteLength > 最大首包字节) {
			throw new Error('VLESS 首包超过长度限制');
		}
		首包缓存 = 拼接字节(首包缓存, 字节);
		const 请求头 = 解析VLESS请求(首包缓存, uuid字节);
		if (请求头 === null) return;
		if (!请求头) {
			关闭(1008, 'invalid VLESS request');
			return;
		}

		const 目标IP = await 解析IPv4(请求头.主机);
		const 候选代理 = await 获取SSTP候选节点(sstp代理);
		let 最后错误 = null;
		for (const 代理 of 候选代理.slice(0, 节点连接尝试上限)) {
			try {
				目标连接 = await 建立SSTP连接(代理, 目标IP, 请求头.端口);
				break;
			} catch (错误) {
				最后错误 = 错误;
				console.warn(`[SSTP Worker] 节点连接失败 ${代理.hostname}:${代理.port}:`, 错误);
			}
		}
		if (!目标连接) throw new Error(`所有 SSTP 候选节点均连接失败: ${最后错误?.message || 'empty node list'}`);
		目标连接.closed?.catch(() => { });
		目标写入器 = 目标连接.writable.getWriter();

		发送(new Uint8Array([请求头.版本, 0]));
		const 有效载荷 = 首包缓存.subarray(请求头.头长度);
		首包缓存 = 空字节;
		if (有效载荷.byteLength) await 目标写入器.write(有效载荷);
		void 转发下行();
	};

	const 接收 = 数据 => {
		处理队列 = 处理队列.then(() => 处理数据(数据)).catch(报错并关闭);
	};

	const 早期数据 = 请求.headers.get('sec-websocket-protocol');
	if (早期数据) {
		try {
			const 解码结果 = 解码早期数据(早期数据);
			if (解码结果) 接收(解码结果);
		} catch (错误) {
			console.error('[SSTP Worker] WebSocket 早期数据无效:', 错误);
			关闭(1008, 'invalid early data');
		}
	}

	服务端.addEventListener('message', 事件 => 接收(事件.data));
	服务端.addEventListener('close', () => 关闭());
	服务端.addEventListener('error', 事件 => 报错并关闭(事件.error || new Error('WebSocket error')));

	const 响应头 = 早期数据 && /^[A-Za-z0-9_-]+={0,2}$/.test(早期数据)
		? { 'sec-websocket-protocol': 早期数据 }
		: undefined;
	return 响应头
		? new Response(null, { status: 101, webSocket: 客户端, headers: 响应头 })
		: new Response(null, { status: 101, webSocket: 客户端 });
}

function 解析VLESS请求(字节, uuid字节) {
	if (字节.byteLength < 18) return null;
	let uuid差异 = 0;
	for (let 索引 = 0; 索引 < 16; 索引++) uuid差异 |= 字节[索引 + 1] ^ uuid字节[索引];
	if (uuid差异 !== 0) return false;

	const 版本 = 字节[0];
	const 附加长度 = 字节[17];
	const 命令偏移 = 18 + 附加长度;
	if (字节.byteLength < 命令偏移 + 4) return null;
	if (字节[命令偏移] !== 1) return false;

	const 端口 = (字节[命令偏移 + 1] << 8) | 字节[命令偏移 + 2];
	const 地址类型 = 字节[命令偏移 + 3];
	let 地址偏移 = 命令偏移 + 4;
	let 主机;

	if (地址类型 === 1) {
		if (字节.byteLength < 地址偏移 + 4) return null;
		主机 = [...字节.subarray(地址偏移, 地址偏移 + 4)].join('.');
		地址偏移 += 4;
	} else if (地址类型 === 2) {
		if (字节.byteLength < 地址偏移 + 1) return null;
		const 域名长度 = 字节[地址偏移++];
		if (字节.byteLength < 地址偏移 + 域名长度) return null;
		主机 = 解码器.decode(字节.subarray(地址偏移, 地址偏移 + 域名长度));
		地址偏移 += 域名长度;
	} else if (地址类型 === 3) {
		return false;
	} else {
		return false;
	}

	if (!端口 || !主机) return false;
	return { 版本, 主机, 端口, 头长度: 地址偏移 };
}

async function 转换为字节(数据) {
	if (数据 instanceof Uint8Array) return 数据;
	if (数据 instanceof ArrayBuffer) return new Uint8Array(数据);
	if (ArrayBuffer.isView(数据)) return new Uint8Array(数据.buffer, 数据.byteOffset, 数据.byteLength);
	if (typeof Blob !== 'undefined' && 数据 instanceof Blob) return new Uint8Array(await 数据.arrayBuffer());
	throw new Error('仅支持二进制 VLESS WebSocket 数据');
}

function 解码早期数据(数据) {
	if (数据.length > Math.ceil(最大早期数据字节 * 4 / 3) + 4) throw new Error('早期数据过长');
	if (!/^[A-Za-z0-9_-]+={0,2}$/.test(数据)) throw new Error('早期数据不是 base64url');
	const base64 = 数据.replace(/-/g, '+').replace(/_/g, '/').replace(/=+$/, '');
	const 补齐 = base64.padEnd(Math.ceil(base64.length / 4) * 4, '=');
	const 原始 = atob(补齐);
	if (原始.length > 最大早期数据字节) throw new Error('早期数据过长');
	return Uint8Array.from(原始, 字符 => 字符.charCodeAt(0));
}

async function 解析IPv4(主机) {
	if (符合IPv4(主机)) return 主机;
	if (主机.includes(':')) throw new Error('SSTP IPv4 隧道不支持 IPv6 目标地址');

	const 响应 = await fetch(`https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(主机)}&type=A`, {
		headers: { accept: 'application/dns-json' }
	});
	if (!响应.ok) throw new Error(`DNS 查询失败: HTTP ${响应.status}`);
	const 结果 = await 响应.json();
	const 地址 = 结果.Answer?.find(记录 => 记录.type === 1 && 符合IPv4(记录.data))?.data;
	if (!地址) throw new Error(`无法将目标域名解析为 IPv4 地址: ${主机}`);
	return 地址;
}

async function 获取SSTP候选节点(代理) {
	if (!代理.nodesUrl) return [代理];
	const 节点列表 = await 读取SSTP节点列表(代理.nodesUrl);
	const 候选节点 = [...节点列表];
	for (let 索引 = 候选节点.length - 1; 索引 > 0; 索引--) {
		const 随机数 = crypto.getRandomValues(new Uint32Array(1))[0];
		const 交换索引 = 随机数 % (索引 + 1);
		[候选节点[索引], 候选节点[交换索引]] = [候选节点[交换索引], 候选节点[索引]];
	}
	return 候选节点.map(节点 => ({
		...节点,
		username: 代理.username,
		password: 代理.password
	}));
}

async function 读取SSTP节点列表(网址) {
	if (缓存节点列表 && Date.now() < 缓存节点到期) return 缓存节点列表;
	if (刷新节点列表任务) return 刷新节点列表任务;

	刷新节点列表任务 = (async () => {
		const 响应 = await 带超时(fetch(网址, { headers: { accept: 'text/plain' } }), SSTP连接超时, '读取 SSTP 节点清单超时');
		if (!响应.ok) throw new Error(`读取 SSTP 节点清单失败: HTTP ${响应.status}`);
		const 内容 = await 带超时(响应.text(), SSTP连接超时, '读取 SSTP 节点清单内容超时');
		if (内容.length > 节点列表最大字节) throw new Error('SSTP 节点清单超过大小限制');

		const 节点 = [];
		const 已见节点 = new Set();
		for (const 行 of 内容.split(/\r?\n/)) {
			const 匹配 = /^([a-z0-9.-]+):(\d{1,5})$/i.exec(行.trim());
			if (!匹配) continue;
			const 主机名 = 匹配[1].toLowerCase();
			const 端口 = Number(匹配[2]);
			if (
				主机名.length > 253 ||
				主机名.startsWith('.') ||
				主机名.endsWith('.') ||
				主机名.includes('..') ||
				!主机名.split('.').every(标签 => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i.test(标签)) ||
				!Number.isInteger(端口) ||
				端口 < 1 ||
				端口 > 65535
			) continue;
			const 键 = `${主机名}:${端口}`;
			if (已见节点.has(键)) continue;
			已见节点.add(键);
			节点.push({ hostname: 主机名, port: 端口 });
			if (节点.length >= 500) break;
		}
		if (!节点.length) throw new Error('SSTP 节点清单中没有有效的 host:port');
		缓存节点列表 = 节点;
		缓存节点到期 = Date.now() + 节点列表缓存时长;
		return 节点;
	})();

	try {
		return await 刷新节点列表任务;
	} catch (错误) {
		if (!缓存节点列表) throw 错误;
		console.warn('[SSTP Worker] 节点清单暂不可用，继续使用上次缓存:', 错误);
		缓存节点到期 = Date.now() + 60_000;
		return 缓存节点列表;
	} finally {
		刷新节点列表任务 = null;
	}
}

function 符合IPv4(地址) {
	const 部分 = String(地址).split('.');
	return 部分.length === 4 && 部分.every(值 => /^\d{1,3}$/.test(值) && Number(值) >= 0 && Number(值) <= 255);
}

async function 建立SSTP连接(代理, 目标主机, 目标端口) {
	let 缓冲区 = 空字节, 协议编号 = 1, 套接字 = null, 读取器 = null, 写入器 = null;
	let 已完成关闭 = false, 关闭完成, 拒绝关闭;
	const 已关闭 = new Promise((resolve, reject) => {
		关闭完成 = resolve;
		拒绝关闭 = reject;
	});
	const 结束关闭 = (处理, 值) => {
		if (已完成关闭) return;
		已完成关闭 = true;
		处理(值);
	};
	const 关闭 = () => {
		try { 读取器?.cancel?.().catch?.(() => { }); } catch { }
		try { 读取器?.releaseLock?.(); } catch { }
		try { 写入器?.close?.().catch?.(() => { }); } catch { }
		try { 写入器?.releaseLock?.(); } catch { }
		try { 套接字?.close?.(); } catch { }
		结束关闭(关闭完成);
	};

	const 读取数据块 = async () => {
		const { value, done } = await 读取器.read();
		if (done || !value) throw new Error('SSTP 套接字已关闭');
		return 转换为字节(value);
	};
	const 读取字节 = async 长度 => {
		while (缓冲区.byteLength < 长度) {
			const 数据块 = await 读取数据块();
			缓冲区 = 缓冲区.byteLength ? 拼接字节(缓冲区, 数据块) : 数据块;
		}
		const 结果 = 缓冲区.subarray(0, 长度);
		缓冲区 = 缓冲区.subarray(长度);
		return 结果;
	};
	const 读取文本行 = async () => {
		for (;;) {
			const 行尾 = 缓冲区.indexOf(10);
			if (行尾 >= 0) {
				const 行 = 解码器.decode(缓冲区.subarray(0, 行尾));
				缓冲区 = 缓冲区.subarray(行尾 + 1);
				return 行.replace(/\r$/, '');
			}
			const 数据块 = await 读取数据块();
			缓冲区 = 缓冲区.byteLength ? 拼接字节(缓冲区, 数据块) : 数据块;
		}
	};
	const 读取数据包 = async (超时 = SSTP连接超时) => {
		const 头 = await 带超时(读取字节(4), 超时, 'SSTP 读取超时');
		const 长度 = 读取十六位(头, 2) & 0x0fff;
		if (长度 < 4) throw new Error('SSTP 数据包长度无效');
		return {
			控制包: (头[1] & 1) !== 0,
			内容: 长度 > 4 ? await 带超时(读取字节(长度 - 4), 超时, 'SSTP 数据包内容读取超时') : 空字节
		};
	};
	const 封装SSTP数据 = 协议帧 => {
		const 长度 = 6 + 协议帧.byteLength;
		const 数据包 = new Uint8Array(长度);
		数据包.set([0x10, 0x00, ((长度 >> 8) & 0x0f) | 0x80, 长度 & 0xff, 0xff, 0x03]);
		数据包.set(协议帧, 6);
		return 数据包;
	};
	const 构造PPP配置 = (协议, 代码, 编号, 选项 = []) => {
		const 选项长度 = 选项.reduce((总长, 项目) => 总长 + 2 + 项目.data.byteLength, 0);
		const 帧 = new Uint8Array(6 + 选项长度);
		const 视图 = new DataView(帧.buffer);
		视图.setUint16(0, 协议);
		帧[2] = 代码;
		帧[3] = 编号;
		视图.setUint16(4, 4 + 选项长度);
		选项.reduce((偏移, 项目) => {
			帧[偏移] = 项目.type;
			帧[偏移 + 1] = 2 + 项目.data.byteLength;
			帧.set(项目.data, 偏移 + 2);
			return 偏移 + 2 + 项目.data.byteLength;
		}, 6);
		return 帧;
	};
	const 解析PPP = 数据 => {
		const 偏移 = 数据.byteLength >= 2 && 数据[0] === 0xff && 数据[1] === 0x03 ? 2 : 0;
		if (数据.byteLength - 偏移 < 4) return null;
		const 协议 = 读取十六位(数据, 偏移);
		if (协议 === 0x0021) return { 协议, IP包: 数据.subarray(偏移 + 2) };
		if (数据.byteLength - 偏移 < 6) return null;
		return {
			协议,
			代码: 数据[偏移 + 2],
			内容: 数据.subarray(偏移 + 6),
			原始帧: 数据.subarray(偏移)
		};
	};
	const 解析PPP选项 = 数据 => {
		const 选项 = [];
		for (let 偏移 = 0; 偏移 + 2 <= 数据.byteLength;) {
			const 类型 = 数据[偏移], 长度 = 数据[偏移 + 1];
			if (长度 < 2 || 偏移 + 长度 > 数据.byteLength) break;
			选项.push({ 类型, 数据: 数据.subarray(偏移 + 2, 偏移 + 长度) });
			偏移 += 长度;
		}
		return 选项;
	};

	try {
		const 主机 = 代理.hostname.replace(/^\[|\]$/g, '');
		套接字 = connect({ hostname: 主机, port: 代理.port }, { secureTransport: 'on', allowHalfOpen: false });
		await 带超时(套接字.opened, SSTP连接超时, 'SSTP 服务连接超时');
		读取器 = 套接字.readable.getReader();
		写入器 = 套接字.writable.getWriter();

		const 显示主机 = 主机.includes(':') ? `[${主机}]` : 主机;
		const HTTP请求 = 编码器.encode(
			`SSTP_DUPLEX_POST /sra_{BA195980-CD49-458b-9E23-C84EE0ADCD75}/ HTTP/1.1\r\n`
			+ `Host: ${代理.port === 443 ? 显示主机 : `${显示主机}:${代理.port}`}\r\n`
			+ 'Content-Length: 18446744073709551615\r\n'
			+ `SSTPCORRELATIONID: {${crypto.randomUUID()}}\r\n\r\n`
		);
		const 协议 = new Uint8Array(2);
		new DataView(协议.buffer).setUint16(0, 1);
		const 最大接收单元 = new Uint8Array(2);
		new DataView(最大接收单元.buffer).setUint16(0, 1500);
		const SSTP连接请求 = new Uint8Array(14);
		const SSTP视图 = new DataView(SSTP连接请求.buffer);
		SSTP连接请求[0] = 0x10;
		SSTP连接请求[1] = 0x01;
		SSTP视图.setUint16(2, SSTP连接请求.byteLength | 0x8000);
		SSTP视图.setUint16(4, 0x0001);
		SSTP视图.setUint16(6, 1);
		SSTP连接请求[9] = 1;
		SSTP视图.setUint16(10, 4 + 协议.byteLength);
		SSTP连接请求.set(协议, 12);

		await 带超时(写入器.write(拼接字节(
			HTTP请求,
			SSTP连接请求,
			封装SSTP数据(构造PPP配置(0xc021, 1, 协议编号++, [{ type: 1, data: 最大接收单元 }]))
		)), SSTP连接超时, 'SSTP HTTP 握手请求超时');
		const 状态行 = await 带超时(读取文本行(), SSTP连接超时, 'SSTP HTTP 握手超时');
		for (;;) {
			if (await 带超时(读取文本行(), SSTP连接超时, 'SSTP HTTP 响应头超时') === '') break;
		}
		if (!/HTTP\/\d(?:\.\d)?\s+2\d\d/i.test(状态行)) {
			throw new Error(`SSTP HTTP 握手失败: ${状态行 || 'invalid status'}`);
		}

		let 本地LCP已确认 = false, 对端LCP已确认 = false, 需要PAP = false;
		let 已发送PAP = false, PAP已完成 = false, 已开始IPCP = false, IPCP已完成 = false, 本地地址 = null;
		const 发送PAP = async () => {
			if (!本地LCP已确认 || !对端LCP已确认 || !需要PAP || 已发送PAP) return;
			if (代理.username === null || 代理.password === null) throw new Error('SSTP 服务器要求用户名和密码认证');
			const 用户名 = 编码器.encode(代理.username), 密码 = 编码器.encode(代理.password);
			if (用户名.byteLength > 255 || 密码.byteLength > 255) throw new Error('SSTP 用户名或密码过长');
			const 长度 = 6 + 用户名.byteLength + 密码.byteLength;
			const 帧 = new Uint8Array(2 + 长度), 视图 = new DataView(帧.buffer);
			视图.setUint16(0, 0xc023);
			帧[2] = 1;
			帧[3] = 协议编号++;
			视图.setUint16(4, 长度);
			帧[6] = 用户名.byteLength;
			帧.set(用户名, 7);
			帧[7 + 用户名.byteLength] = 密码.byteLength;
			帧.set(密码, 8 + 用户名.byteLength);
			await 带超时(写入器.write(封装SSTP数据(帧)), SSTP连接超时, 'SSTP PAP 认证请求超时');
			已发送PAP = true;
		};
		const 开始IPCP = async () => {
			if (!本地LCP已确认 || !对端LCP已确认 || 已开始IPCP || (需要PAP && !PAP已完成)) return;
			await 带超时(写入器.write(封装SSTP数据(构造PPP配置(0x8021, 1, 协议编号++, [
				{ type: 3, data: new Uint8Array(4) }
			]))), SSTP连接超时, 'SSTP IPCP 请求超时');
			已开始IPCP = true;
		};

		for (let 轮次 = 0; 轮次 < 50 && !IPCP已完成; 轮次++) {
			const 数据包 = await 读取数据包();
			if (数据包.控制包) continue;
			const PPP = 解析PPP(数据包.内容);
			if (!PPP) continue;

			if (PPP.协议 === 0xc021) {
				if (PPP.代码 === 1) {
					const 认证选项 = 解析PPP选项(PPP.内容).find(选项 => 选项.类型 === 3);
					if (认证选项?.数据?.byteLength >= 2) {
						const 认证协议 = 读取十六位(认证选项.数据);
						if (认证协议 !== 0xc023) throw new Error(`SSTP 不支持 PPP 认证协议: 0x${认证协议.toString(16)}`);
						需要PAP = true;
					}
					const 确认帧 = new Uint8Array(PPP.原始帧);
					确认帧[2] = 2;
					await 带超时(写入器.write(封装SSTP数据(确认帧)), SSTP连接超时, 'SSTP LCP 确认超时');
					对端LCP已确认 = true;
					await 发送PAP();
					await 开始IPCP();
				} else if (PPP.代码 === 2) {
					本地LCP已确认 = true;
					await 发送PAP();
					await 开始IPCP();
				}
				continue;
			}

			if (PPP.协议 === 0xc023) {
				if (PPP.代码 === 2) {
					PAP已完成 = true;
					await 开始IPCP();
				} else if (PPP.代码 === 3) {
					throw new Error('SSTP PAP 认证失败');
				}
				continue;
			}

			if (PPP.协议 === 0x8021) {
				if (PPP.代码 === 1) {
					const 确认帧 = new Uint8Array(PPP.原始帧);
					确认帧[2] = 2;
					await 带超时(写入器.write(封装SSTP数据(确认帧)), SSTP连接超时, 'SSTP IPCP 确认超时');
					await 开始IPCP();
				} else if (PPP.代码 === 3) {
					const 地址选项 = 解析PPP选项(PPP.内容).find(选项 => 选项.类型 === 3);
					if (地址选项?.数据?.byteLength === 4) {
						本地地址 = [...地址选项.数据].join('.');
						await 带超时(写入器.write(封装SSTP数据(构造PPP配置(0x8021, 1, 协议编号++, [
							{ type: 3, data: 地址选项.数据 }
						]))), SSTP连接超时, 'SSTP IPCP 地址请求超时');
						已开始IPCP = true;
					}
				} else if (PPP.代码 === 2) {
					const 地址选项 = 解析PPP选项(PPP.内容).find(选项 => 选项.类型 === 3);
					if (地址选项?.数据?.byteLength === 4) 本地地址 = [...地址选项.数据].join('.');
					IPCP已完成 = true;
				}
			}
		}
		if (!本地地址) throw new Error('SSTP 未分配 IPv4 地址');

		const 源端口 = 10000 + (随机十六位() % 50000);
		const 源地址字节 = new Uint8Array(本地地址.split('.').map(Number));
		const 目标地址字节 = new Uint8Array(目标主机.split('.').map(Number));
		let 序列号 = 读取三十二位(crypto.getRandomValues(new Uint8Array(4)));
		let 确认序号 = 0;
		const IP头模板 = new Uint8Array(20);
		IP头模板.set([0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 64, 6]);
		IP头模板.set(源地址字节, 12);
		IP头模板.set(目标地址字节, 16);
		const TCP伪首部 = new Uint8Array(1432);
		TCP伪首部.set(源地址字节);
		TCP伪首部.set(目标地址字节, 4);
		TCP伪首部[9] = 6;

		const 构造TCP帧 = (标志, 载荷 = 空字节) => {
			const 数据 = 载荷 instanceof Uint8Array ? 载荷 : new Uint8Array(载荷);
			const TCP长度 = 20 + 数据.byteLength;
			const IP长度 = 20 + TCP长度;
			const SSTP长度 = 8 + IP长度;
			const 帧 = new Uint8Array(SSTP长度);
			const 视图 = new DataView(帧.buffer);
			帧.set([0x10, 0x00, ((SSTP长度 >> 8) & 0x0f) | 0x80, SSTP长度 & 0xff, 0xff, 0x03, 0x00, 0x21]);
			帧.set(IP头模板, 8);
			视图.setUint16(10, IP长度);
			视图.setUint16(12, 随机十六位());
			视图.setUint16(18, 互联网校验和(帧, 8, 20));
			视图.setUint16(28, 源端口);
			视图.setUint16(30, 目标端口);
			视图.setUint32(32, 序列号);
			视图.setUint32(36, 确认序号);
			帧[40] = 0x50;
			帧[41] = 标志;
			视图.setUint16(42, 65535);
			if (数据.byteLength) 帧.set(数据, 48);
			TCP伪首部[10] = TCP长度 >> 8;
			TCP伪首部[11] = TCP长度 & 0xff;
			TCP伪首部.set(帧.subarray(28, 28 + TCP长度), 12);
			视图.setUint16(44, 互联网校验和(TCP伪首部, 0, 12 + TCP长度));
			return 帧;
		};
		const 匹配TCP包 = IP包 => {
			if (IP包.byteLength < 40 || IP包[9] !== 6) return null;
			const IP头长 = (IP包[0] & 0x0f) * 4;
			if (IP包.byteLength < IP头长 + 20) return null;
			if (读取十六位(IP包, IP头长) !== 目标端口 || 读取十六位(IP包, IP头长 + 2) !== 源端口) return null;
			return {
				标志: IP包[IP头长 + 13],
				序列号: 读取三十二位(IP包, IP头长 + 4),
				载荷偏移: IP头长 + ((IP包[IP头长 + 12] >> 4) & 0x0f) * 4
			};
		};

		await 带超时(写入器.write(构造TCP帧(0x02)), SSTP连接超时, 'SSTP TCP SYN 发送超时');
		序列号 = (序列号 + 1) >>> 0;
		let TCP已建立 = false;
		for (let 尝试 = 0; 尝试 < 30; 尝试++) {
			const 数据包 = await 读取数据包();
			if (数据包.控制包) continue;
			const PPP = 解析PPP(数据包.内容);
			if (!PPP || PPP.协议 !== 0x0021) continue;
			const TCP = 匹配TCP包(PPP.IP包);
			if (!TCP || (TCP.标志 & 0x12) !== 0x12) continue;
			确认序号 = (TCP.序列号 + 1) >>> 0;
			await 带超时(写入器.write(构造TCP帧(0x10)), SSTP连接超时, 'SSTP TCP ACK 发送超时');
			TCP已建立 = true;
			break;
		}
		if (!TCP已建立) throw new Error('SSTP TCP 握手超时');

		let 流控制器 = null;
		const readable = new ReadableStream({
			start(控制器) { 流控制器 = 控制器; },
			cancel() { 关闭(); }
		});
		void (async () => {
			try {
				let 待发数据 = [], 待发长度 = 0;
				const 刷新 = () => {
					if (!待发长度) return;
					流控制器.enqueue(待发数据.length === 1 ? 待发数据[0] : 拼接字节(...待发数据));
					待发数据 = [];
					待发长度 = 0;
					写入器.write(构造TCP帧(0x10)).catch(() => { });
				};
				for (;;) {
					const 数据包 = await 读取数据包(60_000);
					if (数据包.控制包) continue;
					const PPP = 解析PPP(数据包.内容);
					if (!PPP || PPP.协议 !== 0x0021) continue;
					const TCP = 匹配TCP包(PPP.IP包);
					if (!TCP) continue;

					if (TCP.载荷偏移 < PPP.IP包.byteLength) {
						const 数据 = PPP.IP包.subarray(TCP.载荷偏移);
						if (数据.byteLength) {
							确认序号 = (TCP.序列号 + 数据.byteLength) >>> 0;
							待发数据.push(new Uint8Array(数据));
							待发长度 += 数据.byteLength;
						}
					}
					if (TCP.标志 & 0x01) {
						刷新();
						确认序号 = (确认序号 + 1) >>> 0;
						写入器.write(构造TCP帧(0x11)).catch(() => { });
						try { 流控制器.close(); } catch { }
						关闭();
						return;
					}
					if (缓冲区.byteLength < 4 || 待发长度 >= 32768) 刷新();
				}
			} catch (错误) {
				try { 流控制器?.error(错误); } catch { }
				结束关闭(拒绝关闭, 错误);
				try { 套接字?.close?.(); } catch { }
			}
		})();

		const writable = new WritableStream({
			async write(数据) {
				const 字节 = await 转换为字节(数据);
				if (!字节.byteLength) return;
				if (字节.byteLength <= TCP最大分段) {
					await 写入器.write(构造TCP帧(0x18, 字节));
					序列号 = (序列号 + 字节.byteLength) >>> 0;
					return;
				}
				const 帧列表 = [];
				for (let 偏移 = 0; 偏移 < 字节.byteLength; 偏移 += TCP最大分段) {
					const 分段 = 字节.subarray(偏移, Math.min(偏移 + TCP最大分段, 字节.byteLength));
					帧列表.push(构造TCP帧(0x18, 分段));
					序列号 = (序列号 + 分段.byteLength) >>> 0;
				}
				await 写入器.write(拼接字节(...帧列表));
			},
			close() {
				return 写入器.write(构造TCP帧(0x11)).catch(() => { });
			},
			abort(错误) {
				关闭();
				if (错误) 结束关闭(拒绝关闭, 错误);
			}
		});

		return { readable, writable, closed: 已关闭, close: 关闭 };
	} catch (错误) {
		关闭();
		throw 错误;
	}
}

function 拼接字节(...列表) {
	if (!列表.length) return 空字节;
	const 总长 = 列表.reduce((总数, 数据) => 总数 + 数据.byteLength, 0);
	const 结果 = new Uint8Array(总长);
	let 偏移 = 0;
	for (const 数据 of 列表) {
		结果.set(数据, 偏移);
		偏移 += 数据.byteLength;
	}
	return 结果;
}

function 读取十六位(字节, 偏移 = 0) {
	return (字节[偏移] << 8) | 字节[偏移 + 1];
}

function 读取三十二位(字节, 偏移 = 0) {
	return ((字节[偏移] << 24) | (字节[偏移 + 1] << 16) | (字节[偏移 + 2] << 8) | 字节[偏移 + 3]) >>> 0;
}

function 随机十六位() {
	return 读取十六位(crypto.getRandomValues(new Uint8Array(2)));
}

function 互联网校验和(字节, 偏移, 长度) {
	let 总和 = 0;
	for (let 索引 = 偏移; 索引 < 偏移 + 长度 - 1; 索引 += 2) 总和 += 读取十六位(字节, 索引);
	if (长度 & 1) 总和 += 字节[偏移 + 长度 - 1] << 8;
	while (总和 >> 16) 总和 = (总和 & 0xffff) + (总和 >> 16);
	return (~总和) & 0xffff;
}

async function 带超时(任务, 毫秒, 消息) {
	let 定时器;
	try {
		return await Promise.race([
			任务,
			new Promise((_, reject) => { 定时器 = setTimeout(() => reject(new Error(消息)), 毫秒); })
		]);
	} finally {
		clearTimeout(定时器);
	}
}