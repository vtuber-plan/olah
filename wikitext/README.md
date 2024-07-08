---
annotations_creators:
- no-annotation
language_creators:
- crowdsourced
language:
- en
license:
- cc-by-sa-3.0
- gfdl
multilinguality:
- monolingual
size_categories:
- 1M<n<10M
source_datasets:
- original
task_categories:
- text-generation
- fill-mask
task_ids:
- language-modeling
- masked-language-modeling
paperswithcode_id: wikitext-2
pretty_name: WikiText
dataset_info:
- config_name: wikitext-103-raw-v1
  features:
  - name: text
    dtype: string
  splits:
  - name: test
    num_bytes: 1305088
    num_examples: 4358
  - name: train
    num_bytes: 546500949
    num_examples: 1801350
  - name: validation
    num_bytes: 1159288
    num_examples: 3760
  download_size: 315466397
  dataset_size: 548965325
- config_name: wikitext-103-v1
  features:
  - name: text
    dtype: string
  splits:
  - name: test
    num_bytes: 1295575
    num_examples: 4358
  - name: train
    num_bytes: 545141915
    num_examples: 1801350
  - name: validation
    num_bytes: 1154751
    num_examples: 3760
  download_size: 313093838
  dataset_size: 547592241
- config_name: wikitext-2-raw-v1
  features:
  - name: text
    dtype: string
  splits:
  - name: test
    num_bytes: 1305088
    num_examples: 4358
  - name: train
    num_bytes: 11061717
    num_examples: 36718
  - name: validation
    num_bytes: 1159288
    num_examples: 3760
  download_size: 7747362
  dataset_size: 13526093
- config_name: wikitext-2-v1
  features:
  - name: text
    dtype: string
  splits:
  - name: test
    num_bytes: 1270947
    num_examples: 4358
  - name: train
    num_bytes: 10918118
    num_examples: 36718
  - name: validation
    num_bytes: 1134123
    num_examples: 3760
  download_size: 7371282
  dataset_size: 13323188
configs:
- config_name: wikitext-103-raw-v1
  data_files:
  - split: test
    path: wikitext-103-raw-v1/test-*
  - split: train
    path: wikitext-103-raw-v1/train-*
  - split: validation
    path: wikitext-103-raw-v1/validation-*
- config_name: wikitext-103-v1
  data_files:
  - split: test
    path: wikitext-103-v1/test-*
  - split: train
    path: wikitext-103-v1/train-*
  - split: validation
    path: wikitext-103-v1/validation-*
- config_name: wikitext-2-raw-v1
  data_files:
  - split: test
    path: wikitext-2-raw-v1/test-*
  - split: train
    path: wikitext-2-raw-v1/train-*
  - split: validation
    path: wikitext-2-raw-v1/validation-*
- config_name: wikitext-2-v1
  data_files:
  - split: test
    path: wikitext-2-v1/test-*
  - split: train
    path: wikitext-2-v1/train-*
  - split: validation
    path: wikitext-2-v1/validation-*
---

# Dataset Card for "wikitext"

## Table of Contents
- [Dataset Description](#dataset-description)
  - [Dataset Summary](#dataset-summary)
  - [Supported Tasks and Leaderboards](#supported-tasks-and-leaderboards)
  - [Languages](#languages)
- [Dataset Structure](#dataset-structure)
  - [Data Instances](#data-instances)
  - [Data Fields](#data-fields)
  - [Data Splits](#data-splits)
- [Dataset Creation](#dataset-creation)
  - [Curation Rationale](#curation-rationale)
  - [Source Data](#source-data)
  - [Annotations](#annotations)
  - [Personal and Sensitive Information](#personal-and-sensitive-information)
- [Considerations for Using the Data](#considerations-for-using-the-data)
  - [Social Impact of Dataset](#social-impact-of-dataset)
  - [Discussion of Biases](#discussion-of-biases)
  - [Other Known Limitations](#other-known-limitations)
- [Additional Information](#additional-information)
  - [Dataset Curators](#dataset-curators)
  - [Licensing Information](#licensing-information)
  - [Citation Information](#citation-information)
  - [Contributions](#contributions)

## Dataset Description

- **Homepage:** [https://blog.einstein.ai/the-wikitext-long-term-dependency-language-modeling-dataset/](https://blog.einstein.ai/the-wikitext-long-term-dependency-language-modeling-dataset/)
- **Repository:** [More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)
- **Paper:** [Pointer Sentinel Mixture Models](https://arxiv.org/abs/1609.07843)
- **Point of Contact:** [Stephen Merity](mailto:smerity@salesforce.com)
- **Size of downloaded dataset files:** 391.41 MB
- **Size of the generated dataset:** 1.12 GB
- **Total amount of disk used:** 1.52 GB

### Dataset Summary

 The WikiText language modeling dataset is a collection of over 100 million tokens extracted from the set of verified
 Good and Featured articles on Wikipedia. The dataset is available under the Creative Commons Attribution-ShareAlike License.

Compared to the preprocessed version of Penn Treebank (PTB), WikiText-2 is over 2 times larger and WikiText-103 is over
110 times larger. The WikiText dataset also features a far larger vocabulary and retains the original case, punctuation
and numbers - all of which are removed in PTB. As it is composed of full articles, the dataset is well suited for models
that can take advantage of long term dependencies.

Each subset comes in two different variants:
- Raw (for character level work) contain the raw tokens, before the addition of the <unk> (unknown) tokens.
- Non-raw (for word level work) contain only the tokens in their vocabulary (wiki.train.tokens, wiki.valid.tokens, and wiki.test.tokens).
  The out-of-vocabulary tokens have been replaced with the the <unk> token.


### Supported Tasks and Leaderboards

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

### Languages

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

## Dataset Structure

### Data Instances

#### wikitext-103-raw-v1

- **Size of downloaded dataset files:** 191.98 MB
- **Size of the generated dataset:** 549.42 MB
- **Total amount of disk used:** 741.41 MB

An example of 'validation' looks as follows.
```
This example was too long and was cropped:

{
    "text": "\" The gold dollar or gold one @-@ dollar piece was a coin struck as a regular issue by the United States Bureau of the Mint from..."
}
```

#### wikitext-103-v1

- **Size of downloaded dataset files:** 190.23 MB
- **Size of the generated dataset:** 548.05 MB
- **Total amount of disk used:** 738.27 MB

An example of 'train' looks as follows.
```
This example was too long and was cropped:

{
    "text": "\" Senjō no Valkyria 3 : <unk> Chronicles ( Japanese : 戦場のヴァルキュリア3 , lit . Valkyria of the Battlefield 3 ) , commonly referred to..."
}
```

#### wikitext-2-raw-v1

- **Size of downloaded dataset files:** 4.72 MB
- **Size of the generated dataset:** 13.54 MB
- **Total amount of disk used:** 18.26 MB

An example of 'train' looks as follows.
```
This example was too long and was cropped:

{
    "text": "\" The Sinclair Scientific Programmable was introduced in 1975 , with the same case as the Sinclair Oxford . It was larger than t..."
}
```

#### wikitext-2-v1

- **Size of downloaded dataset files:** 4.48 MB
- **Size of the generated dataset:** 13.34 MB
- **Total amount of disk used:** 17.82 MB

An example of 'train' looks as follows.
```
This example was too long and was cropped:

{
    "text": "\" Senjō no Valkyria 3 : <unk> Chronicles ( Japanese : 戦場のヴァルキュリア3 , lit . Valkyria of the Battlefield 3 ) , commonly referred to..."
}
```

### Data Fields

The data fields are the same among all splits.

#### wikitext-103-raw-v1
- `text`: a `string` feature.

#### wikitext-103-v1
- `text`: a `string` feature.

#### wikitext-2-raw-v1
- `text`: a `string` feature.

#### wikitext-2-v1
- `text`: a `string` feature.

### Data Splits

|       name        | train |validation|test|
|-------------------|------:|---------:|---:|
|wikitext-103-raw-v1|1801350|      3760|4358|
|wikitext-103-v1    |1801350|      3760|4358|
|wikitext-2-raw-v1  |  36718|      3760|4358|
|wikitext-2-v1      |  36718|      3760|4358|

## Dataset Creation

### Curation Rationale

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

### Source Data

#### Initial Data Collection and Normalization

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

#### Who are the source language producers?

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

### Annotations

#### Annotation process

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

#### Who are the annotators?

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

### Personal and Sensitive Information

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

## Considerations for Using the Data

### Social Impact of Dataset

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

### Discussion of Biases

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

### Other Known Limitations

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

## Additional Information

### Dataset Curators

[More Information Needed](https://github.com/huggingface/datasets/blob/master/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

### Licensing Information

The dataset is available under the [Creative Commons Attribution-ShareAlike License (CC BY-SA 4.0)](https://creativecommons.org/licenses/by-sa/4.0/).

### Citation Information

```
@misc{merity2016pointer,
      title={Pointer Sentinel Mixture Models},
      author={Stephen Merity and Caiming Xiong and James Bradbury and Richard Socher},
      year={2016},
      eprint={1609.07843},
      archivePrefix={arXiv},
      primaryClass={cs.CL}
}
```


### Contributions

Thanks to [@thomwolf](https://github.com/thomwolf), [@lewtun](https://github.com/lewtun), [@patrickvonplaten](https://github.com/patrickvonplaten), [@mariamabarham](https://github.com/mariamabarham) for adding this dataset.