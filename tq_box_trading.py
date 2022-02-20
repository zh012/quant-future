from asyncio.log import logger
import os
import execution_manager as em
from tabulate import tabulate
import typer

strategy_name = os.path.splitext(os.path.split(__file__)[1])[0]
strategy_home = em.normabspath(f"~/.quant-future/{strategy_name}")


config_schema = {
    "contract.name": "合约名称",
    "resistance.upper": "阻力带上沿",
    "resistance.lower": "阻力带下沿",
    "support.upper": "支撑带上沿",
    "support.lower": "支撑带下沿",
    "stop_loss_percent": "止损点百分点",
    "tq.username": "易信账号 https://account.shinnytech.com/",
    "tq.password": "易信账号密码",
    "br.name": "期货公司",
    "br.account": "期货公司账户",
    "br.password": "期货公司密码",
    "tr.mode": "交易账户类型: sim / paper / real",
    "tel.bot": None,
    "tel.channel": None,
    "disable_desktop_notification": False,
}


def strategy(e: em.Execution):
    from tqsdk import TqApi, TqAuth, TqAccount, TqKq, TqSim

    e.logger.info("SDK imported")

    config = e.read_config()

    # config the notifier
    if bot := config.get("tel.bot"):
        telegram = {"bot": bot, "channel": config.get("tel.channel")}
    else:
        telegram = None
    desktop = not config.get("disable_desktop_notification")
    noti = em.Notifier(logger=e.logger, telegram=telegram, desktop=desktop)
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

    # start the quant strategy
    symbol = config["contract.name"]
    resistance_upper = int(config["resistance.upper"])
    resistance_lower = int(config["resistance.lower"])
    support_upper = int(config["support.upper"])
    support_lower = int(config["support.lower"])
    stop_loss_percent = int(config["stop_loss_percent"])

    # 主连代码
    # quote = api.get_quote("KQ.m@SHFE.cu")
    quote = api.get_quote(symbol)
    order = api.get_order()

    e.logger.info(quote)
    e.logger.info(order)

    # order = api.insert_order(
    #     symbol=symbol,
    #     direction="BUY",
    #     offset="OPEN",
    #     limit_price=71470,
    #     volume=2,
    # )

    import time

    while True:
        noti.send(f"震荡策略 {e.name}", f"{symbol} 价格进入支撑带 {quote.last_price}")
        time.sleep(5)
        # api.wait_update()
        # e.logger.info(f"{quote.instrument_name} {quote.datetime} {quote.last_price}")


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
