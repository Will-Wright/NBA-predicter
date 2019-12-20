# NBA Predicter

This project collects NBA game and season statistics to predict individual game win/loss outcomes.  This project also compares various machine learning classifier algorithms to demonstrate which are most accurate.

The data pipeline is split into two classes.  The `DataProcessor` class handles acquisition, integration, and processing.  The `DataClassifier` class handles modeling (selecting features, params, classifiers), classifying, evaluation, and plotting results.

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


## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* **Billie Thompson** - *Initial work* - [PurpleBooth](https://github.com/PurpleBooth)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc
