"""测试数据库中的标的数据"""
from vnpy.trader.database import get_database

database = get_database()
overviews = database.get_bar_overview()

print(f"数据库中共有 {len(overviews)} 条记录")
print("\n按标的分组:")

# 去重
symbol_dict = {}
for overview in overviews:
    key = (overview.symbol, overview.exchange)
    if key not in symbol_dict:
        symbol_dict[key] = overview

print(f"去重后共有 {len(symbol_dict)} 个标的")
print("\n前20个标的:")

sorted_symbols = sorted(symbol_dict.values(), 
                       key=lambda x: (x.exchange.value, x.symbol))

for i, overview in enumerate(sorted_symbols[:20], 1):
    print(f"{i}. {overview.symbol}.{overview.exchange.value}")

print(f"\n... 共 {len(sorted_symbols)} 个标的")

