import os
import execution_manager as em


strategy_name = os.path.splitext(os.path.split(__file__)[1])[0]
strategy_home = em.normabspath(f"~/.quant-future/{strategy_name}")


config_schema = {
    "tq.username": "易信账号 https://account.shinnytech.com/",
    "tq.password": "易信账号密码",
    "contract.name": "合约名称",
    "resistance.upper": "阻力带上沿",
    "resistance.lower": "阻力带下沿",
    "support.upper": "支撑带上沿",
    "support.lower": "支撑带下沿",
    "stop_loss_percent": "止损点百分点",
}


def strategy(e: em.Execution):
    from tqsdk import TqApi, TqAuth

    config = e.read_config()
    auth = TqAuth(config["tq.username"], config["tq.password"])
    api = TqApi(auth=auth)
    # api = TqApi(web_gui=True, auth=auth)
    symbol = config["contract.name"]
    resistance_upper = int(config["resistance.upper"])
    resistance_lower = int(config["resistance.lower"])
    support_upper = int(config["support.upper"])
    support_lower = int(config["support.lower"])
    stop_loss_percent = int(config["stop_loss_percent"])

    # quote = api.get_quote(symbol)
    quote = api.get_quote("KQ.m@SHFE.cu")
    print(quote)
    print(quote.underlying_quote)
    print(quote.underlying_symbol)

    # k1 = api.get_tick_serial("SHFE.cu2203", 10)
    # k2 = api.get_kline_serial("SHFE.ni2206", 10)

    while True:
        api.wait_update()
        print(quote)
        print(quote.underlying_quote)
        print(quote.underlying_symbol)
        print()


app = em.App(
    home=strategy_home,
    name=strategy_name,
    default_config=config_schema,
    runner=strategy,
)


# @app.cli.command(name="list")
# def better_status():
#     print("overwrite")


if __name__ == "__main__":
    app.cli()
