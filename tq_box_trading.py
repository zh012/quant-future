import os
import execution_manager as em


strategy_name = os.path.splitext(os.path.split(__file__)[1])[0]
strategy_home = em.normabspath(f"~/.quant-future/{strategy_name}")


config_schema = {
    "tq.username": "易信账号 https://account.shinnytech.com/",
    "tq.password": "易信账号密码",
    "contract.symbol": "合约名称",
    "resistance.upper": "阻力带上沿",
    "resistance.lower": "阻力带下沿",
    "support.upper": "支撑带上沿",
    "support.lower": "支撑带下沿",
    "stop_loss_percent": "止损点百分点",
}

# from tqsdk import TqApi, TqAuth


def strategy(e: em.Execution):
    import time

    print(e.read_text(e.config_file))

    print(e.read_config())

    number = 0
    while True:
        e.logger.info(number)
        number += 1
        time.sleep(5)

    # api = TqApi(web_gui=True, auth=TqAuth("zh012", "Goat2015"))
    # k1 = api.get_tick_serial("SHFE.cu2203", 10)
    # k2 = api.get_kline_serial("SHFE.ni2206", 10)

    # while True:
    #     api.wait_update()
    #     # if api.is_changing(k1.iloc[-1], "datetime"):
    #     print(k1.iloc[-1])


app = em.App(
    home=strategy_home,
    name=strategy_name,
    default_config=config_schema,
    runner=strategy,
)


if __name__ == "__main__":
    app.cli()
