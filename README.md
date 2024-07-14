### Overview
Our group has partnered with Johnson County's Mental Health Center (JoCo MHC) with a specific goal in mind: to reduce the likelihood of individuals with mental health issues returning to the criminal justice system. This collaborative effort aims to minimize the negative consequences associated with both crime and incarceration.

To achieve this, our data science team is working with the JoCo MHC. Our primary objective is to assist them in identifying individuals who would benefit the most from a proactive mental health treatment program. By pinpointing those individuals who face a higher risk of reoffending due to their mental health challenges, we will empower JoCo MHC to direct their intervention resources more effectively.

In essence, we aim to lower the chances of individuals with untreated mental health conditions re-engaging in criminal behavior. This, in turn, will contribute to a reduction in overall crime rates and help break the pernicious cycle of incarceration.

### Background and Goals
Untreated mental health conditions can lead to a damaging cycle, often resulting in repeated incarceration. This cycle of recidivism has severe consequences for both individuals and the community. Surveys have also shown that a significant proportion of inmates suffer from mental health and substance abuse issues. Despite these high levels of need, the criminal justice system is ill-equipped to address them, exacerbating the cycle.

The need to address this issue is clear. Incarceration and untreated mental health conditions have serious consequences, impacting both individuals and society as a whole due to increased criminal activity. Each successful intervention in this program will not only enhance the life of a Johnson County resident but also promote a sense of safety among their neighbors and conserve resources that would otherwise be used for their incarceration. Preventive treatment, especially when accurately targeted, is a more efficient and just allocation of resources compared to post-offense punishment.

Our data science team aims to employ predictive analytics to identify the individuals in Johnson County who would benefit most from the MHC's outreach program. Ideally, these individuals should have both a high risk of near-term recidivism and unmet mental health needs. This effort will enhance the MHC's ability to run their pilot mental health outreach program efficiently. Our primary objective is to accurately pinpoint those individuals with a high likelihood of reoffending due to their mental health condition. However, we must also consider equity concerns. If our training data doesn't represent the county's diversity, our model may not perform equally well for all residents, inadvertently excluding certain groups from treatment. Additionally, we should account for treatment efficiency. Identifying the most manageable conditions for the MHC to address would enable them to assist more people with the same resources. To balance these objectives effectively, ongoing collaboration and communication with the MHC and other stakeholders will be essential.

### Existing Treatment We Are Hoping to Improve Upon
In the United States, individuals at risk of incarceration face significant challenges in accessing mental health support and services. Consequently, many county residents are currently grappling with untreated mental health issues, which often play a role in their eventual incarceration.

The county's current approach primarily is to provide mental health services to individuals who are already incarcerated. Identifying incarcerated individuals is more straightforward due to the availability of screening forms and surveillance tools. However, this approach is generally ineffective because it only intervenes after a crime has occurred, and incarceration conditions are often unsuitable for addressing and treating mental health problems.

The success of the MHC pilot program would underscore the idea that preventing mental health conditions through proactive treatment is a more effective and just approach to reforming the criminal justice system compared to the current reactive model.

### Data
We have access to a variety of data sources from different agencies within Johnson County. These data sources include:

- **Incarceration Data**: Information on the duration of an individual's jail time, the reasons for their incarceration, details about the bail and trial processes, and more. We have data on around 180,000 distinct jail inmates for the years 1987 through 2019.
- **Mental Health Data**: Information regarding individuals' mental health diagnosis status, frequency and reasons for contacting the mental health center, insurance information, mental health exam scores, and related information. Data includes the results of 3 standardized screening surveys taken at different stages of the criminal justice process:
  - Pre-trial assessment. We have pre-trial assessment results for 18,046 individuals (12,481 ‘no’ and 5,565 ‘yes’ for MH need);
  - Brief Jail Mental Health Screen (BJMHS). Results are available for 24,100 inmates (who took it at least once) with column bjmhs_referred indicating MH help need;
  - Level of Service Inventory - Revised (LSIR). 22,762 survey results are available for 17,148 inmates with 93% of surveys indicating MH need identified.
- **First Responder Data**: Our dataset provides information about encounters with ambulances and police officers, along with descriptive details of what these first responders observed during these encounters. MEDACT incidents capture 2010-2019 data for more than 300,000 ambulance records (9,750 records with MH related issues) for around 180,000 distinct individuals (8,505 with MH related issues). There is also data on 110,000 PD arrests for the same period with 68,486 distinct individuals involved.
- **Demographic Data**: Our dataset describes individuals' income, race, gender, approximate residential location, age, and other demographic characteristics.

We believe that the current data we possess is likely sufficient for effective modeling purposes. However, we remain open to the possibility of supplementing it with additional data as it becomes available. One potential avenue we've considered is incorporating more data from government census databases to gain a more comprehensive understanding of residents' living conditions and environments.

### Proposed Analysis Plan
Our primary deliverable to the MHC will be a predictive model designed to identify individuals who are most likely to meet two specific criteria:

1. Re-offending (being incarcerated again) within a specified time frame (i.e. one year)
2. Having an untreated mental health condition.

Detecting the presence of an untreated or any mental health condition in individuals is a complex task. In our dataset, multiple sources of contact with Johnson County services provide indicators that someone might be grappling with mental health issues. These sources include:

- Expressing mental health distress in responses to the "Brief Jail Mental Health Screen" inmate survey (BJMHS).
- Noting substance use disorders or mental health conditions on a pre-trial assessment form.
- Exhibiting signs of mental health distress during interactions with first responders.
- Engagement with the JoCo Medical Health Center (MHC), particularly if it resulted in a diagnosis of a mental health condition.

While we have several ways to identify mental health conditions, it's likely that some individuals with these conditions may not have interacted with Johnson County's services before reoffending. Although it might be more straightforward to predict recidivism for individuals with a known history of mental health challenges, if this model is deployed on a larger scale to the general population, it's crucial to include individuals for whom we have no prior mental health information. Therefore, we won't filter our dataset to exclude those without past mental health data; instead, we'll consider the occurrence of mental health challenges as one of several predictive features.

To put it in technical terms, our approach involves merging our dataset to ensure that each individual is represented by a single row containing all pertinent information about their life up to a specific point in time, denoted as "t." Achieving this necessitates substantial feature engineering, requiring us to aggregate numerous details into time-invariant data points for each person. One strategy we may use to accomplish this is to aggregate by counts, maximums, or unique occurrences across time. We may also include some features that denote “the most recent” event, or time since the most recent event.

Subsequently, we will employ all these features to predict the likelihood of an individual being incarcerated within a future time frame. Importantly, we will exercise caution to prevent data leakage, meaning that we will exclusively use historical data to forecast future instances of incarceration. To assess the accuracy of our model, we will analyze the actual events that occurred in an individual's life as documented in our training data.

### Model Validation
We will validate our class project by specifically looking at the 100 most likely predictions our model returns for a test dataset, and then seeing how many of those people actually reoffended and had instances of mental health problems. We will compare this to the baseline method of choosing which is selecting a random subsample. The difference in precision between the two approaches will validate the effectiveness of our model. We will also have to systematically look at the demographics of who our model consistently predicts and see if there are any equity issues that need to be addressed.

When considering the real-world deployment of this model, it's crucial to carefully evaluate its effectiveness. One approach is to wait for a period during which the model generates predictions and then assess its performance by comparing these predictions to actual outcomes. However, evaluating the model's effectiveness in a deployed scenario can be challenging without a proper counterfactual.

For instance, if our model chooses to intervene with an individual who subsequently reoffends, it raises the question: was this a failure of the model or simply a limitation of the preventive mental health outreach? To address this, we could conduct two simultaneous studies, where individuals are either randomly assigned interventions or assigned based on the model's predictions, allowing for outcome comparisons, similar to a randomized controlled trial. However, this approach may introduce ethical concerns and consume valuable county resources, which policymakers may not find justifiable.

### Ethics
**Data Privacy**: Safeguarding the confidentiality of the data we use for predictions is essential. This data contains sensitive health and incarceration information, and to protect privacy, it has already been hashed to remove Personally Identifiable Information (PII). Additionally, all our work is conducted within a secure computing environment to further mitigate the risk of a data breach.

**Model Transparency**: Ensuring transparency in our model's decision-making process is also important. Policymakers and residents will understandably want to understand why they are or aren't prioritized by city services. To achieve this, we can conduct feature importance analysis to illustrate the most influential factors behind our predictions. In cases where transparency is of the highest priority, we may consider using more interpretable models to provide clearer insights into the decision-making process.

**Bias and Equity**: As previously mentioned, our training dataset might not accurately represent the entire population of Johnson County, potentially affecting the effectiveness of our predictions for different demographic groups. To address this, we could explore data resampling techniques to balance our dataset. Furthermore, it's crucial to collaborate with the county to define an equitable distribution strategy that aligns with their priorities. This strategy could involve ensuring that various demographic groups receive similar levels of care, even if it means sacrificing some efficiency. This distribution approach could be informed by demographics or geographic factors. In any case, it's essential to establish clear communication about these requirements before finalizing our modeling efforts.
