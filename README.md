# NBA Predicter

This project collects NBA game and season statistics to predict individual game win/loss outcomes.  This project also compares various machine learning classifier algorithms to demonstrate which are most accurate.

The data pipeline is split into two classes.  The `DataProcessor` class handles acquisition, integration, and processing.  The `DataClassifier` class handles modeling (selecting features, params, classifiers), classifying, evaluation, and plotting results.

 <p align="center">
 <img src="./results/classifier_results.png">
 </p>
 <p align="center">
 Original image
 </p>


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

This project uses the following packages

```
sklearn
pandas
seaborn
basketball_reference_web_scraper
```

You can find the web scraper at https://github.com/jaebradley/basketball_reference_web_scraper.


### Running Tests

To run the entire data pipeline on your local machine, just follow the [NBA Predictor Notebook](https://github.com/Will-Wright/NBA-predicter/blob/master/NBA%20Predicter.ipynb)


## Future work

- Add new data to dataset: advanced statistics, number games on the road, etc.
- Use [WEKA machine learning models](https://www.cs.waikato.ac.nz/ml/weka/)