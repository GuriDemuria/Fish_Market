from fish_market import *

def test_extract():
    data = extract(bucket, prefix)

    page_1 = pd.read_csv(s3_client.get_object(Bucket=bucket, Key='python/fish-market.csv')['Body'])
    page_2 = pd.read_csv(s3_client.get_object(Bucket=bucket, Key='python/fish-market-mon.csv')['Body'])
    page_3 = pd.read_csv(s3_client.get_object(Bucket=bucket, Key='python/fish-market-tues.csv')['Body'])

    assert len(data) == (len(page_1) + len(page_2) + len(page_3))
    assert set(data['Species'].unique()) == set.union(set(page_1['Species'].unique()),
                                                      set(page_2['Species'].unique()),
                                                      set(page_3['Species'].unique()))
    for i in (data.groupby('Species').max()).head():
        for j in data['Species'].unique():
            assert (data.groupby('Species').max())[i][j] == max((page_1.groupby('Species').max())[i][j],
                                                                (page_2.groupby('Species').max())[i][j],
                                                                (page_3.groupby('Species').max())[i][j])

    for i in (data.groupby('Species').min()).head():
        for j in data['Species'].unique():
            assert (data.groupby('Species').min())[i][j] == min((page_1.groupby('Species').min())[i][j],
                                                                (page_2.groupby('Species').min())[i][j],
                                                                (page_3.groupby('Species').min())[i][j])

    for i in (data.groupby('Species').count()).head():
        for j in data['Species'].unique():
            assert (data.groupby('Species').count())[i][j] == (sum([(page_1.groupby('Species').count())[i][j],
                                                                   (page_2.groupby('Species').count())[i][j],
                                                                   (page_3.groupby('Species').count())[i][j]]))

def test_transform():
    data = transform(extract(bucket, prefix))
    data1 = extract(bucket, prefix)

    for i in data.head():
        for j in list(data.index):
            assert data[i][j] == (data1.groupby('Species').mean())[i][j]

def test_load():
    bucket1 = s3_resource.Bucket(bucket)
    contents = bucket1.objects.all()
    file_names = [item.key for item in contents]
    assert 'Data26/fish/GuriD.csv' in file_names






