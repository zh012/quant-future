import asyncio
from asyncio.log import logger
from contextlib import closing
import math
import os
import time
import traceback
import execution_manager as em
from tabulate import tabulate
import typer

import pendulum


def today():
    # 北京时间晚8点以后算第二天
    return pendulum.now().in_timezone("Asia/Shanghai").add(hours=4).date()


def hour():
    return pendulum.now().in_timezone("Asia/Shanghai").hour


def time_str():
    return pendulum.now().in_timezone("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss")


strategy_name = os.path.splitext(os.path.split(__file__)[1])[0]
strategy_home = em.normabspath(f"~/.quant-future/{strategy_name}")

# 沪铜主连代码 KQ.m@SHFE.cu
# 沪铜合约代码 SHFE.cu2203
config_schema = {
    "contract.name": "合约名称",
    "resistance": "阻力位",
    "support": "支撑位",
    "budget": "本金",
    "tq.username": "zh012",
    "tq.password": "易信账号密码",
    "br.name": "期货公司",
    "br.account": "期货公司账户",
    "br.password": "期货公司密码",
    "tr.mode": "paper",
    "tel.bot": "",
    "tel.channel": "1686949643",
    "desktop.notification": False,
}


def today_target(total_pos: int, current_pos: int, steps: int) -> int:
    quo, rem = total_pos // steps, total_pos % steps
    stagger = [0]
    for i in range(1, steps + 1):
        stagger.append(stagger[i - 1] + quo + (rem and 1))
        if rem > 0:
            rem -= 1
    for i, t in enumerate(stagger):
        if current_pos < t:
            return t
    else:
        return total_pos


_notifier = None


def get_notifier(e: em.Execution) -> em.Notifier:
    global _notifier

    if not _notifier:
        config = e.read_config()
        symbol = config["contract.name"]
        if bot := config.get("tel.bot"):
            telegram = {"bot": bot, "channel": config.get("tel.channel")}
        else:
            telegram = None
        desktop = config.get("desktop.notification")
        _notifier = em.Notifier(
            logger=e.logger, telegram=telegram, desktop=desktop, title=f"震荡策略{symbol}"
        )
        _notifier.start()
    return _notifier


def get_api(e: em.Execution):
    from tqsdk import (
        TqApi,
        TqAuth,
        TqAccount,
        TqKq,
        TqSim,
        TargetPosTask,
        TargetPosScheduler,
        TqMultiAccount,
    )

    e.logger.info("SDK imported")

    config = e.read_config()
    auth = TqAuth(config["tq.username"], config["tq.password"])

    tr_mode = config.get("tr.mode", "sim")
    if tr_mode == "real":
        broker_account = TqAccount(
            config["br.name"], config["br.account"], config["br.password"]
        )
    elif tr_mode == "paper":
        broker_account = TqKq()
    else:
        broker_account = TqSim()

    port = em.get_free_port()
    e.write_text("__gui__", f"http://127.0.0.1:{port}")
    return TqApi(
        account=broker_account,
        auth=auth,
        web_gui=f":{port}",
        debug=e.file("tq-debug.txt"),
    )


_strategy_exiting = False


def strategy(e: em.Execution):
    global _strategy_exiting
    from tqsdk import TargetPosTask

    noti = get_notifier(e)
    api = get_api(e)

    config = e.read_config()
    symbol = config["contract.name"]
    resistance = int(config["resistance"])
    support = int(config["support"])
    budget = int(config["budget"])
    buy_range = [support * 0.99, support * 1.01]
    stop_loss = support * 0.985

    # 上交所黄金不能使用市价单
    # pos_task = TargetPosTask(
    #     api, symbol, price=lambda d: d == "BUY" and buy_range[1] or quote.bid_price1
    # )
    pos_task = TargetPosTask(
        api,
        symbol,
    )

    with closing(api):
        position = api.get_position(symbol)
        quote = api.get_quote(symbol)
        total_target_pos = round(budget * 0.2 / (support * quote.volume_multiple * 0.1))
        notified_target = None

        def close_position():
            while position.pos_long != 0:
                pos_task.set_target_volume(0)
                api.wait_update()
            noti.send(f"{time_str()} 平仓结束\n策略退出")

        if _strategy_exiting:
            close_position()
            return

        noti.send(
            f"{time_str()} 策略启动\n总资金:{budget}\n入场价:{buy_range}\n止盈价:{resistance}\n止损价:{stop_loss}\n总目标仓位:{total_target_pos}手\n昨仓:{position.pos_long_his}手\n今仓:{position.pos_long_today}手"
        )
        while True:
            api.wait_update()

            today_target_pos = today_target(total_target_pos, position.pos_long_his, 5)

            if api.is_changing(quote, "last_price"):
                if quote.last_price > buy_range[0] and quote.last_price < buy_range[1]:
                    pos_task.set_target_volume(today_target_pos)
                    if notified_target != today_target_pos:
                        notified_target = today_target_pos
                        noti.send(
                            f"{time_str()} 加仓\n总目标仓位:{total_target_pos}手\n已有仓位:{position.pos_long}手\n今日仓位目标:{today_target_pos}手"
                        )
                elif quote.last_price <= stop_loss:
                    pos_task.set_target_volume(0)
                    noti.send(
                        f"{time_str()}\n平仓止损\n总目标仓位:{total_target_pos}手\n已有仓位:{position.pos_long}手\n今日仓位目标:0手"
                    )
                    break
                elif quote.last_price >= resistance:
                    pos_task.set_target_volume(0)
                    noti.send(
                        f"{time_str()}\n平仓止盈\n总目标仓位:{total_target_pos}手\n已有仓位:{position.pos_long}手\n今日仓位目标:0手"
                    )
                    break

            if api.is_changing(position, "pos_long"):
                noti.send(
                    f"{time_str()} 仓位变动\n总目标仓位:{total_target_pos}手\n已有仓位:{position.pos_long}手\n今日仓位目标:{today_target_pos}手"
                )

        close_position()


def strategy_with_retry(e: em.Execution):
    backoff = int(e.read_config().get("retry.backoff", 3 * 60))
    noti = get_notifier(e)

    while True:
        try:
            strategy(e)
            break
        except Exception as e:
            noti.send(noti.send(f"{time_str()} 程序异常\n{backoff}秒后重启\n{e}"))
            time.sleep(backoff)


app = em.App(
    home=strategy_home,
    name=strategy_name,
    default_config=config_schema,
    runner=strategy_with_retry,
)


@app.cli.command(name="list")
def status():
    def info(e: em.Execution):
        s = e.status()
        gui = ""
        if s == em.ExecutionStatus.running:
            try:
                gui = e.read_text("__gui__")
            except:
                gui = ""
        config = e.read_config()

        return e.name, config.get("contract.name", ""), s.tr_zh(), gui

    data = [info(e) for e in app.execution_list()]

    typer.echo(tabulate(data, headers=["震荡策略", "标的合约", "状态", "监控"]))
    typer.echo()


if __name__ == "__main__":
    app.cli()
