import logging
import pandas as pd 

logging.basicConfig(filename='../log/log.log', encoding='utf-8', level=logging.DEBUG)
log = logging.getLogger(__name__)


def post_extraction(json_path):
    
    df_in = pd.read_json(json_path, orient="table")
    log.info("data loaded into a dataframe")
    df_in = df_in.loc[df_in['type']=="journal"].dropna()
    
    # petite 'triche' car je n'ai pas eu le temps de prétraiter les input donc les sorties sont mauvaises
    df_in['journal'] = df_in["value"].apply(lambda x: x[0:28])
    
    grp = (df_in.drop("value", axis=1).groupby(["journal", "drug"]).count())
    grp2 = grp.reset_index()[["journal", "drug"]].groupby("journal").count().sort_values("drug", ascending=False)
    return grp2.head(1)


if __name__ == "__main__":
    print(post_extraction("../output/test.json"))
    
