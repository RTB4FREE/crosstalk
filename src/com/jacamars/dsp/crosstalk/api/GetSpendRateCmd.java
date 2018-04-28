package com.jacamars.dsp.crosstalk.api;


import com.jacamars.dsp.crosstalk.budget.BudgetController;

/**
 * Get the spend rate of a campaign or campaign/creative
 * 
 * @author Ben M. Faul
 *
 */
public class GetSpendRateCmd extends ApiCommand {

	/** The total spend */
	public double totalSpend;
	/** The current daily spend */
	public double dailySpend;
	/** The current hourly spend */
	public double hourlySpend;
	/** The current minute spend average */
	public double minuteSpendAverage;
	/** Standard deviation of the average minute spend */
	public double std;

	/**
	 * Default constructor for the object.
	 */

	public GetSpendRateCmd() {

	}

	/**
	 * Basic form of the command.
	 * 
	 * @param username
	 *            String. The username to use for authorization.
	 * @param password
	 *            String. The password to use for authorization.
	 */
	public GetSpendRateCmd(String username, String password) {
		super(username, password);
		type = SpendRate;

	}

	/**
	 * Targeted form of the command.
	 * 
	 * @param username
	 *            String. The user authorization.
	 * @param password
	 *            String. THe password authorization.
	 * @param campaign
	 *            String. The target campaign.
	 * @param creative
	 *            String. The target creative.
	 * @param type
	 *            String. The type of the creative, eg 'banner'
	 */
	public GetSpendRateCmd(String username, String password, String campaign, String creative, String type) {
		super(username, password);
		this.campaign = campaign;
		this.creative = creative;
		this.type = type;
		type = SpendRate;
	}

	/**
	 * Convert to JSON
	 */
	public String toJson() throws Exception {
		return mapper.writeValueAsString(this);
	}

	/**
	 * Executes the command, marshalls the response.
	 */
	@Override
	public void execute() {
		super.execute();

		try {
			if (campaign == null) {
				totalSpend = BudgetController.getInstance().getCampaignTotalSpend(null);
				dailySpend = BudgetController.getInstance().getCampaignDailySpend(null);
				hourlySpend = BudgetController.getInstance().getCampaignHourlySpend(null);
				minuteSpendAverage = BudgetController.getInstance().getCampaignSpendAverage(null);
				std = BudgetController.getInstance().getStdDeviationMinuteSpend(null);
			} else {
				if (creative == null) {
					totalSpend = BudgetController.getInstance().getCampaignTotalSpend(campaign);
					dailySpend = BudgetController.getInstance().getCampaignDailySpend(campaign);
					hourlySpend = BudgetController.getInstance().getCampaignHourlySpend(campaign);
					minuteSpendAverage = BudgetController.getInstance().getCampaignSpendAverage(campaign);
					std = BudgetController.getInstance().getStdDeviationMinuteSpend(campaign);
				} else {
					totalSpend = BudgetController.getInstance().getCreativeTotalSpend(campaign, creative, type);
					dailySpend = BudgetController.getInstance().getCreativeDailySpend(campaign, creative, type);
					hourlySpend = BudgetController.getInstance().getCreativeHourlySpend(campaign, creative, type);
					minuteSpendAverage = BudgetController.getInstance().getCreativeSpendAverage(campaign, creative,
							type);
					std = BudgetController.getInstance().getStdDeviationMinuteSpend(campaign, creative, type);
				}
			}
		} catch (Exception e) {
			error = true;
			message = e.getMessage();
		}
	}
}
