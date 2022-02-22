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
    def stagger(total_pos: int, steps: int) -> list[int]:
        quo, rem = total_pos // steps, total_pos % steps
        stagger = [0]
        for i in range(1, steps + 1):
            stagger.append(stagger[i - 1] + quo + (rem and 1))
            if rem > 0:
                rem -= 1
        return stagger

    def re_stagger(s: list[int], current_pos: int) -> int:
        for i, t in enumerate(s):
            if current_pos <= t:
                break
        rem_steps = len(s) - i
        rem_pos = s[-1] - current_pos
        return stagger(rem_pos, rem_steps)

    s = stagger(total_pos, steps)
    if current_pos != 0:
        s = re_stagger(s, current_pos)
    return s[1]


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
        # start the quant strategy
        position = api.get_position(symbol)
        quote = api.get_quote(symbol)

        total_target_pos = round(budget * 0.2 / (support * quote.volume_multiple * 0.1))
        current_pos = position.pos_long_his
        today_pos = position.pos_long_today
        today_date = today()

        buy_range = [support * 0.99, support * 1.01]
        stop_loss = support * 0.985

        # initiate status
        if e.db["status"].count() == 0:
            e.db["status"].insert(
                dict(
                    symbol=symbol,
                    strategy_started=today_date,
                    budget=budget,
                    support=support,
                    resistance=resistance,
                    target_pos=total_target_pos,
                    current_pos=current_pos,
                    updated_at=pendulum.now("UTC"),
                    created_at=pendulum.now("UTC"),
                )
            )
            e.db["event"].insert(
                dict(
                    event="started",
                    date=today_date,
                    symbol=symbol,
                    budget=budget,
                    support=support,
                    resistance=resistance,
                    target_pos=total_target_pos,
                    current_pos=current_pos,
                    pid=os.getpid(),
                    created_at=pendulum.now("UTC"),
                )
            )
            noti.send(
                f"<策略启动>\n总资金: {budget}\n入场价: {buy_range}\n止盈价: {resistance}\n止损价: {stop_loss}"
            )
        else:
            status = e.db["status"].find_one()
            if (symbol, budget, support, resistance) != (
                status["symbol"],
                status["budget"],
                status["support"],
                status["resistance"],
            ):
                e.db["status"].update(
                    dict(
                        id=status["id"],
                        symbol=symbol,
                        budget=budget,
                        support=support,
                        resistance=resistance,
                        target_pos=total_target_pos,
                        current_pos=current_pos,
                        updated_at=pendulum.now("UTC"),
                    ),
                    keys=["id"],
                )
                e.db["event"].insert(
                    dict(
                        event="config_changed",
                        date=today_date,
                        symbol=symbol,
                        budget=budget,
                        support=support,
                        resistance=resistance,
                        total_target_pos=total_target_pos,
                        current_pos=current_pos,
                        pid=os.getpid(),
                        created_at=pendulum.now("UTC"),
                    )
                )
            noti.send(
                f"<策略重启>\n总资金: {budget}\n入场价: {buy_range}\n止盈价: {resistance}\n止损价: {stop_loss}"
            )

        if position.pos_long_his >= total_target_pos or today_pos > 0:
            today_target_pos = None
        else:
            today_target_pos = today_target(total_target_pos, current_pos, 5)

        noti.send(
            f"日期:{time_str()}\n历史仓位:{current_pos}手\n当日仓位:{today_pos}手\n今日目标:{today_target_pos}手"
        )

        pos_task = TargetPosTask(api, symbol, price=lambda d: buy_range[1])

        today_buy, today_close = None, None

        while True:
            api.wait_update()

            if api.is_changing(position):
                e.logger.info(
                    f"position changes: his ({[current_pos, position.pos_long_his]}) today ({[today_pos, position.pos_long_today]})"
                )
                if (
                    current_pos != position.pos_long_his
                    or today_pos != position.pos_long_today
                ):
                    current_pos = position.pos_long_his
                    today_pos = position.pos_long_today
                    e.db["event"].insert(
                        dict(
                            event="position",
                            date=today_date,
                            symbol=symbol,
                            current_pos=current_pos,
                            today_pos=today_pos,
                            pid=os.getpid(),
                            created_at=pendulum.now("UTC"),
                        )
                    )
                    noti.send(
                        f"日期:{time_str()}\n历史仓位:{current_pos}手\n当日仓位:{today_pos}手\n今日目标:{today_target_pos}手"
                    )

            if api.is_changing(quote, "last_price"):
                e.logger.info(f"price changed: {symbol} {quote.last_price})")
                if quote.last_price > buy_range[0] and quote.last_price < buy_range[1]:
                    pos_task.set_target_volume(today_target_pos)
                    # api.insert_order(
                    #     symbol,
                    #     direction="BUY",
                    #     offset="OPEN",
                    #     volume=today_target_pos - current_pos,
                    #     limit_price=quote.ask_price1,
                    # )
                    if today_buy != today_date:
                        today_buy = today_date
                        e.db["event"].insert(
                            dict(
                                event="open_position",
                                date=today_date,
                                symbol=symbol,
                                current_pos=current_pos,
                                today_target_pos=today_target_pos,
                                pid=os.getpid(),
                                created_at=pendulum.now("UTC"),
                            )
                        )
                        noti.send(f"{time_str()}\n加仓 目标{today_target_pos}手")
                elif quote.last_price <= stop_loss or quote.last_price >= resistance:
                    earn = quote.last_price >= resistance
                    pos_task.set_target_volume(0)
                    if today_close != today_date:
                        today_close = today_date
                        e.db["event"].insert(
                            dict(
                                event=earn and "stop_earn" or "stop_loss",
                                date=today_date,
                                symbol=symbol,
                                current_pos=current_pos,
                                pid=os.getpid(),
                                created_at=pendulum.now("UTC"),
                            )
                        )
                        noti.send(f"{time_str()}\n平仓 {earn and '止盈' or '止损'}")
                    break

            new_date = today()
            if today_date != new_date and hour() > 5:
                today_date = new_date
                today_target_pos = today_target(total_target_pos, current_pos, 5)
                e.db["event"].insert(
                    dict(
                        event="new_day",
                        date=today_date,
                        symbol=symbol,
                        today_target_pos=today_target_pos,
                        pid=os.getpid(),
                        created_at=pendulum.now("UTC"),
                    )
                )
                noti.send(
                    f"日期:{time_str()}\n历史仓位:{current_pos}手\n当日仓位:{today_pos}手\n今日目标:{today_target_pos}手"
                )

        e.logger.info(
            f"Strategy exits: {earn and 'earn' or 'loss'}. Closing all positions ..."
        )
        while True:
            api.wait_update()
            if api.is_changing(position):
                e.logger.info(
                    f"pos_long_his: {position.pos_long_his}, pos_long_today: {position.pos_long_today}"
                )
                if position.pos_long_his + position.pos_long_today == 0:
                    break

    e.logger.info("All positions closed.")


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
