from setuptools import setup

# 列出软件包的相关依赖，可以通过'pip install -e’查看
requires = [
    # 'scikit-learn==0.23.2',
    'progressbar==2.5',
]

# 开发人员需要的依赖，`pip install -e ".[dev]"`查看
dev_requires = [

]

setup(
    name='ParallelLineProcess',
    install_requires=requires,
    extras_require={'dev': dev_requires},
    entry_points={
        # 'paste.app_factory': [
        #     'main=covert_vcf2genseries_parallel:main'
        # ]
    },
)
