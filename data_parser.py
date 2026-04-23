# -*- coding: utf-8 -*-
"""
data_parser.py - 麦当劳 Push 日报数据解析模块
支持 Streamlit 上传和本地 CSV 两种模式
"""
import csv, io
from datetime import datetime, timedelta

# 字段名映射（适配新的CSV格式）
COLS = {
    'date': '发送日期',
    'channel': '渠道',
    'ptype': '计划类型',
    'plan_id': 'Plan ID',
    'plan_name': 'Plan名称',
    'owner': '预算owner',
    'coupon': '是否用券',
    'reach_plan': '预计触达',
    'reach': '触达成功',
    'click': '点击人次',
    'order_click': '点击后下单人次',
    'gc': '订单GC',
    'sales': '订单Sales',
    # 消息标题和消息内容不处理，仅用于识别表头
    'msg_title': '消息标题',
    'msg_content': '消息内容'
}


def parse_csv(file_or_path):
    """解析 CSV，返回 (rows_raw, plan_cnt_all, owner_agg, all_dates)

    rows_raw:    date → channel → ptype → metrics
    plan_cnt_all: date → channel → set(plan_id)
    owner_agg:   date → ptype → owner → metrics
    all_dates:   sorted list of dates
    """
    rows_raw    = {}
    plan_cnt_all = {}
    owner_agg   = {}

    # 支持文件对象或路径，多编码兜底（适配多种编码格式）
    if hasattr(file_or_path, 'read'):
        pos = file_or_path.tell() if hasattr(file_or_path, 'tell') else 0
        raw = file_or_path.read()
        if isinstance(raw, bytes):
            for enc in ['utf-8', 'gbk', 'gb2312', 'latin1']:
                try:
                    text = raw.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                text = raw.decode('utf-8', errors='replace')
        else:
            text = raw
        if hasattr(file_or_path, 'seek'):
            file_or_path.seek(pos)
        f = io.StringIO(text)
    else:
        # 本地文件路径，先尝试 utf-8，再尝试 gbk
        for enc in ['utf-8', 'gbk', 'gb2312']:
            try:
                f = open(file_or_path, encoding=enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            f = open(file_or_path, encoding='utf-8', errors='replace')

    reader = csv.DictReader(f, delimiter='\t')  # 使用制表符作为分隔符

    for row in reader:
        # 获取各个字段，确保不为None
        date_val = row.get(COLS['date'])
        channel_val = row.get(COLS['channel'])
        ptype_val = row.get(COLS['ptype'])
        plan_id_val = row.get(COLS['plan_id'])
        
        # 检查关键字段是否存在且非空
        if not date_val or str(date_val).strip() == '' or date_val == COLS['date']:
            continue
        if not channel_val:
            channel_val = '?'
        if not ptype_val:
            ptype_val = 'normal'
        if not plan_id_val:
            continue  # Plan ID 是必需的
        
        d   = str(date_val).strip()
        ch  = str(channel_val).strip()
        pt  = str(ptype_val).strip().lower()
        pid = str(plan_id_val).strip()
        own = str(row.get(COLS['owner'], '')).strip() or '未知'
        
        try:
            # 安全地获取数值字段，处理可能的空值或非数字值
            click_val = row.get(COLS['click'], 0)
            reach_val = row.get(COLS['reach'], 0)
            gc_val = row.get(COLS['gc'], 0)
            sales_val = row.get(COLS['sales'], 0)
            order_click_val = row.get(COLS['order_click'], 0)
            reach_plan_val = row.get(COLS['reach_plan'], 0)
            
            c  = float(click_val) if click_val and str(click_val).strip() != '' else 0.0
            r  = float(reach_val) if reach_val and str(reach_val).strip() != '' else 0.0
            g  = float(gc_val) if gc_val and str(gc_val).strip() != '' else 0.0
            s  = float(sales_val) if sales_val and str(sales_val).strip() != '' else 0.0
            oc = float(order_click_val) if order_click_val and str(order_click_val).strip() != '' else 0.0
            rp = float(reach_plan_val) if reach_plan_val and str(reach_plan_val).strip() != '' else 0.0
        except (ValueError, TypeError):
            continue

        # 标准化日期：去前导零，同时处理不同日期格式
        try:
            d_clean = d.split()[0] if ' ' in d else d  # 只取日期部分，去掉时间
            parts = d_clean.split('/')
            if len(parts) != 3:
                continue  # 日期格式不正确，跳过该行
            d = f"{parts[0]}/{int(parts[1])}/{int(parts[2])}"
        except (IndexError, ValueError):
            continue  # 日期格式错误，跳过该行

        # 初始化嵌套字典结构
        if d not in rows_raw:
            rows_raw[d] = {}
        if ch not in rows_raw[d]:
            rows_raw[d][ch] = {}
        if pt not in rows_raw[d][ch]:
            rows_raw[d][ch][pt] = {
                'click':0,'reach':0,'gc':0,'sales':0,'order_click':0,'reach_plan':0
            }
            
        # 累加指标数据
        for k, v in [('click',c),('reach',r),('gc',g),('sales',s),('order_click',oc),('reach_plan',rp)]:
            rows_raw[d][ch][pt][k] += v
        
        # 计划ID集合统计
        if d not in plan_cnt_all:
            plan_cnt_all[d] = {}
        if ch not in plan_cnt_all[d]:
            plan_cnt_all[d][ch] = set()
        plan_cnt_all[d][ch].add(pid)

        # owner 聚合（S4 数据源）
        if d not in owner_agg:
            owner_agg[d] = {}
        if pt not in owner_agg[d]:
            owner_agg[d][pt] = {}
        if own not in owner_agg[d][pt]:
            owner_agg[d][pt][own] = {
                'click':0,'reach':0,'gc':0,'sales':0,'order_click':0,'reach_plan':0
            }
            
        for k, v in [('click',c),('reach',r),('gc',g),('sales',s),('order_click',oc),('reach_plan',rp)]:
            owner_agg[d][pt][own][k] += v

    f.close()

    def _key(d):
        try:
            p = d.split('/')
            return (int(p[1]), int(p[2]))
        except (IndexError, ValueError):
            return (0, 0)  # 错误日期返回默认值
    all_dates = sorted([d for d in rows_raw.keys() if d], key=_key)
    return rows_raw, plan_cnt_all, owner_agg, all_dates


def calc_date_range(all_dates):
    """从数据中自动计算昨日/前日/周均日期范围
    
    注意：返回的日期格式必须与 parse_csv 中 rows_raw 的 key 一致（去前导零）
    """
    if not all_dates:
        return None, None, []
    latest = all_dates[-1]
    parts = latest.split('/')
    try:
        latest_dt = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, IndexError):
        return None, None, []  # 日期格式错误
    prev_dt = latest_dt - timedelta(days=1)
    DATE_Y = latest  # 最新日期 = 昨日（已是去前导零格式）
    # 前日：去前导零保持一致
    DATE_P = f"{prev_dt.year}/{prev_dt.month}/{prev_dt.day}"
    # 上周7天：必须去前导零，与 rows_raw key 匹配！
    DATE_W = []
    for i in range(1, 8):  # 1~7天前
        d = latest_dt - timedelta(days=i)
        DATE_W.append(f"{d.year}/{d.month}/{d.day}")
    return DATE_Y, DATE_P, DATE_W


def totals_all(rows_raw, dates):
    t = {'click':0,'reach':0,'gc':0,'sales':0,'order_click':0,'reach_plan':0}
    for d in dates:
        if not d or d not in rows_raw:
            continue
        for ch, pts in rows_raw[d].items():
            for pt, vals in pts.items():
                for k in t:
                    t[k] += vals.get(k, 0)
    return t


def ch_totals(rows_raw, ch, dates):
    t = {'click':0,'reach':0,'gc':0,'sales':0,'order_click':0,'reach_plan':0}
    for d in dates:
        if not d or d not in rows_raw or ch not in rows_raw[d]:
            continue
        for pt, vals in rows_raw[d][ch].items():
            for k in t:
                t[k] += vals.get(k, 0)
    return t


def agg_ch_pt(rows_raw, ch, ptype, dates):
    t = {'click':0,'reach':0,'gc':0,'sales':0,'order_click':0,'reach_plan':0}
    for d in dates:
        if not d or d not in rows_raw or ch not in rows_raw[d]:
            continue
        if ptype not in rows_raw[d][ch]:
            continue
        for k, v in rows_raw[d][ch][ptype].items():
            t[k] += v
    return t


def calc_s4_data(owner_agg, DATE_Y, DATE_P, DATE_W):
    """计算 S4 按 Owner 数据
    返回: {
        'aarr':  [{owner, reach_y, reach_p, reach_w, ctr_y, ctr_p, ctr_w, ...}, ...],
        'normal': [...]
    }
    """
    METRICS = ['reach', 'click', 'order_click', 'gc', 'sales', 'reach_plan']

    def _sum(dates, ptype, owner):
        t = {k: 0.0 for k in METRICS}
        for d in dates:
            if not d or d not in owner_agg or ptype not in owner_agg[d]:
                continue
            if owner not in owner_agg[d][ptype]:
                continue
            for k in METRICS:
                t[k] += owner_agg[d][ptype][owner].get(k, 0)
        return t

    def _ctr(m):
        return m['click'] / m['reach'] * 100 if m['reach'] else 0

    result = {}
    for ptype in ['aarr', 'normal']:
        owners = set()
        for d, pts in owner_agg.items():
            if d and ptype in pts:
                owners.update(pts[ptype].keys())

        rows = []
        for owner in sorted(owners):
            yd = _sum([DATE_Y], ptype, owner) if DATE_Y else {k: 0.0 for k in METRICS}
            pd = _sum([DATE_P], ptype, owner) if DATE_P else {k: 0.0 for k in METRICS}
            wd = _sum(DATE_W, ptype, owner) if DATE_W else {k: 0.0 for k in METRICS}
            rows.append({
                'owner': owner,
                'reach_plan_y': yd['reach_plan'],
                'reach_plan_p': pd['reach_plan'],
                'reach_plan_w': wd['reach_plan'] / 7,
                'reach_y': yd['reach'],
                'reach_p': pd['reach'],
                'reach_w': wd['reach'] / 7,
                'click_y': yd['click'],
                'click_p': pd['click'],
                'click_w': wd['click'] / 7,
                'order_click_y': yd['order_click'],
                'order_click_p': pd['order_click'],
                'order_click_w': wd['order_click'] / 7,
                'ctr_y': _ctr(yd),
                'ctr_p': _ctr(pd),
                'ctr_w': _ctr(wd),
                'gc_y': yd['gc'],
                'gc_p': pd['gc'],
                'gc_w': wd['gc'] / 7,
                'sales_y': yd['sales'],
                'sales_p': pd['sales'],
                'sales_w': wd['sales'] / 7,
            })
        result[ptype] = rows

    return result
