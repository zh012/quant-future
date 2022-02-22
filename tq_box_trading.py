from asyncio.log import logger
from contextlib import closing
import math
import os
import execution_manager as em
from tabulate import tabulate
import typer

import pendulum


def today():
    return pendulum.now().in_timezone("Asia/Shanghai").date()


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


def strategy(e: em.Execution):
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
    symbol = config["contract.name"]
    resistance = int(config["resistance"])
    support = int(config["support"])
    budget = int(config["budget"])

    # config the notifier
    if bot := config.get("tel.bot"):
        telegram = {"bot": bot, "channel": config.get("tel.channel")}
    else:
        telegram = None
    desktop = config.get("desktop.notification")
    noti = em.Notifier(
        logger=e.logger, telegram=telegram, desktop=desktop, title=f"震荡策略{symbol}"
    )
    noti.start()

    # config the tq api
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
    api = TqApi(
        account=broker_account,
        auth=auth,
        web_gui=f":{port}",
        debug=e.file("tq-debug.txt"),
    )

    e.logger.info(f"Api initialized. Web gui running at http://127.0.0.1:{port}")

    with closing(api):
        today_volume_set = False
        buy_range = [support * 0.99, support * 1.01]
        stop_loss = support * 0.985
        position = api.get_position(symbol)
        quote = api.get_quote(symbol)
        total_target_pos = round(budget * 0.2 / (support * quote.volume_multiple * 0.1))
        today_target_pos = today_target(total_target_pos, position.pos_long, 5)
        # 上交所黄金不能使用市价单
        # pos_task = TargetPosTask(
        #     api, symbol, price=lambda d: d == "BUY" and buy_range[1] or quote.bid_price1
        # )
        pos_task = TargetPosTask(
            api,
            symbol,
        )
        noti.send(
            f"{time_str()} 策略启动\n总资金:{budget}\n入场价:{buy_range}\n止盈价:{resistance}\n止损价:{stop_loss}\n总目标仓位:{total_target_pos}手\n已有仓位:{position.pos_long}手\n今日目标仓位:{today_target_pos}手"
        )

        today_date = today()
        while True:
            api.wait_update()

            if api.is_changing(quote, "last_price"):
                e.logger.info(f"price changed: {symbol} {quote.last_price})")
                if (
                    position.pos_long_today == 0
                    and today_target_pos != total_target_pos
                    and not today_volume_set
                    and quote.last_price > buy_range[0]
                    and quote.last_price < buy_range[1]
                ):
                    pos_task.set_target_volume(today_target_pos)
                    today_volume_set = True
                    noti.send(
                        f"{time_str()} 加仓\n总目标仓位:{total_target_pos}手\n已有仓位:{position.pos_long}手\n今日仓位目标:{today_target_pos}手"
                    )
                elif quote.last_price <= stop_loss:
                    pos_task.set_target_volume(0)
                    noti.send(f"{time_str()}\n平仓止损")
                    break
                elif quote.last_price >= resistance:
                    pos_task.set_target_volume(0)
                    noti.send(f"{time_str()}\n平仓止盈")
                    break

            if api.is_changing(position, "pos_long"):
                noti.send(
                    f"{time_str()} 仓位变动\n总目标仓位:{total_target_pos}手\n已有仓位:{position.pos_long}手\n今日仓位目标:{today_target_pos}手"
                )

            new_date = today()
            if today_date != new_date and hour() > 5:
                today_date = new_date
                new_target_pos = today_target(total_target_pos, position.pos_long, 5)
                if new_target_pos != today_target_pos:
                    today_target_pos = new_target_pos
                    today_volume_set = False
                    noti.send(
                        f"{time_str()} 仓位目标\n总目标仓位:{total_target_pos}手\n已有仓位:{position.pos_long}手\n今日仓位目标:{today_target_pos}手"
                    )

        while True:
            api.wait_update()
            if position.pos_long == 0:
                noti.send(f"{time_str()} 平仓结束\n策略退出")
                break


app = em.App(
    home=strategy_home,
    name=strategy_name,
    default_config=config_schema,
    runner=strategy,
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
