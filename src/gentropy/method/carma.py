"""CARMA outlier detection method."""

from __future__ import annotations

import concurrent.futures
import warnings
from itertools import combinations
from math import floor, lgamma
from typing import Any

import numpy as np
import pandas as pd
from scipy.linalg import det, inv, pinv
from scipy.optimize import minimize_scalar


class CARMA:
    """Implementation of CARMA outlier detection method."""

    @staticmethod
    def time_limited_CARMA_spike_slab_noEM(
        z: np.ndarray,
        ld: np.ndarray,
        sec_threshold: float = 600,
        tau: float = 0.04,
    ) -> dict[str, Any]:
        """The wrapper for the CARMA_spike_slab_noEM function that runs the function in a separate thread and terminates it if it takes too long.

        Args:
            z (np.ndarray): Numeric vector representing z-scores.
            ld (np.ndarray): Numeric matrix representing the linkage disequilibrium (LD) matrix.
            sec_threshold (float): The time threshold in seconds.
            tau (float): Tuning parameter controlling the level of shrinkage of the LD matrix

        Returns:
            dict[str, Any]: A dictionary containing the following results:
                - PIPs: A numeric vector of posterior inclusion probabilities (PIPs) for all SNPs or None.
                - B_list: A dataframe containing the marginal likelihoods and the corresponding model space or None.
                - Outliers: A list of outlier SNPs or None.
        """
        # Ignore pandas future warnings
        warnings.simplefilter(action="ignore", category=FutureWarning)
        try:
            # Execute CARMA.CARMA_spike_slab_noEM with a timeout
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    CARMA.CARMA_spike_slab_noEM, z=z, ld=ld, tau=tau
                )
                result = future.result(timeout=sec_threshold)
        except concurrent.futures.TimeoutError:
            # If execution exceeds the timeout, return None
            result = {"PIPs": None, "B_list": None, "Outliers": None}

        return result

    @staticmethod
    def CARMA_spike_slab_noEM(
        z: np.ndarray,
        ld: np.ndarray,
        lambda_val: float = 1,
        Max_Model_Dim: int = 200_000,
        all_iter: int = 1,
        all_inner_iter: int = 10,
        epsilon_threshold: float = 1e-5,
        num_causal: int = 10,
        tau: float = 0.04,
        outlier_switch: bool = True,
        outlier_BF_index: float = 1 / 3.2,
    ) -> dict[str, Any]:
        """Perform CARMA analysis using a Spike-and-Slab prior without Expectation-Maximization (EM).

        Args:
            z (np.ndarray): Numeric vector representing z-scores.
            ld (np.ndarray): Numeric matrix representing the linkage disequilibrium (LD) matrix.
            lambda_val (float): Regularization parameter controlling the strength of the L1 penalty.
            Max_Model_Dim (int): Maximum allowed dimension for the causal models.
            all_iter (int): The total number of iterations to run the CARMA analysis.
            all_inner_iter (int): The number of inner iterations in each CARMA iteration.
            epsilon_threshold (float): Threshold for convergence in CARMA iterations.
            num_causal (int): Maximal number of causal variants to be selected in the final model.
            tau (float): Tuning parameter controlling the level of shrinkage of the LD matrix.
            outlier_switch (bool): Whether to consider outlier detection in the analysis.
            outlier_BF_index (float): Bayes Factor threshold for identifying outliers.

        Returns:
            dict[str, Any]: A dictionary containing the following results:
                - PIPs: A numeric vector of posterior inclusion probabilities (PIPs) for all SNPs.
                - B_list: A dataframe containing the marginal likelihoods and the corresponding model space.
                - Outliers: A list of outlier SNPs.
        """
        p_snp = len(z)
        epsilon_list = epsilon_threshold * p_snp
        all_epsilon_threshold = epsilon_threshold * p_snp

        # Zero step
        all_C_list = CARMA._MCS_modified(
            z=z,
            ld_matrix=ld,
            epsilon=epsilon_list,
            Max_Model_Dim=Max_Model_Dim,
            lambda_val=lambda_val,
            outlier_switch=outlier_switch,
            tau=tau,
            num_causal=num_causal,
            inner_all_iter=all_inner_iter,
            outlier_BF_index=outlier_BF_index,
        )

        # Main steps
        for _ in range(0, all_iter):
            ac1 = all_C_list["B_list"]["set_gamma_margin"]
            previous_result = np.mean(ac1[0 : round(len(ac1) / 4)])

            all_C_list = CARMA._MCS_modified(
                z=z,
                ld_matrix=ld,
                input_conditional_S_list=all_C_list["conditional_S_list"],
                Max_Model_Dim=Max_Model_Dim,
                num_causal=num_causal,
                epsilon=epsilon_list,
                outlier_switch=outlier_switch,
                tau=tau,
                lambda_val=lambda_val,
                inner_all_iter=all_inner_iter,
                outlier_BF_index=outlier_BF_index,
            )

            ac1 = all_C_list["B_list"]["set_gamma_margin"]
            difference = np.abs(previous_result - np.mean(ac1[0 : round(len(ac1) / 4)]))
            if difference < all_epsilon_threshold:
                break

        # Calculate PIPs and Credible Set
        pip = CARMA._PIP_func(
            likeli=all_C_list["B_list"]["set_gamma_margin"],
            model_space=all_C_list["B_list"]["matrix_gamma"],
            p=p_snp,
            num_causal=num_causal,
        )

        results_list = {
            "PIPs": pip,
            "B_list": all_C_list["B_list"],
            "Outliers": all_C_list["conditional_S_list"],
        }

        return results_list

    @staticmethod
    def _ind_normal_sigma_fixed_marginal_fun_indi(
        zSigmaz_S: np.ndarray, tau: float, p_S: int, det_S: float
    ) -> float:
        """Internal function for calculating the marginal likelihood of configuration model.

        Args:
            zSigmaz_S (np.ndarray): The zSigmaz_S value.
            tau (float): The tau value.
            p_S (int): The number of SNPs.
            det_S (float): The det_S value.

        Returns:
            float: The marginal likelihood of a model.

        Examples:
            >>> zSigmaz_S = 0.1
            >>> tau = 1 / 0.05**2
            >>> p_S = 3
            >>> det_S = 0.1
            >>> np.round(CARMA._ind_normal_sigma_fixed_marginal_fun_indi(zSigmaz_S, tau, p_S, det_S),decimals=5)
            10.18849
        """
        return p_S / 2.0 * np.log(tau) - 0.5 * np.log(det_S) + zSigmaz_S / 2.0

    @staticmethod
    def _ind_Normal_fixed_sigma_marginal_external(
        index_vec_input: np.ndarray,
        Sigma: np.ndarray,
        z: np.ndarray,
        tau: float,
        p_S: int,
    ) -> float:
        """Marginal likelihood of configuration model.

        Args:
            index_vec_input (np.ndarray): The index vector.
            Sigma (np.ndarray): The Sigma matrix.
            z (np.ndarray): The z vector.
            tau (float): The tau value.
            p_S (int): The number of SNPs.

        Returns:
            float: The marginal likelihood of a model.

        Examples:
            >>> index_vec_input = np.array([1, 2])
            >>> Sigma = np.array([[1, 0.5, 0.2], [0.5, 1, 0.3], [0.2, 0.3, 1]])
            >>> z = np.array([10, 11, 10])
            >>> tau = 1
            >>> p_S = 2
            >>> np.round(CARMA._ind_Normal_fixed_sigma_marginal_external(index_vec_input, Sigma, z, tau, p_S),decimals=5)
            43.60579
        """
        index_vec = index_vec_input - 1
        Sigma_S = Sigma[np.ix_(index_vec, index_vec)]
        A = tau * np.eye(p_S)

        det_S = det(Sigma_S + A)
        Sigma_S_inv = inv(Sigma_S + A)

        sub_z = z[index_vec]
        zSigmaz_S = np.dot(sub_z.T, np.dot(Sigma_S_inv, sub_z))

        b = CARMA._ind_normal_sigma_fixed_marginal_fun_indi(zSigmaz_S, tau, p_S, det_S)

        results = b

        return results

    @staticmethod
    def _outlier_ind_Normal_marginal_external(
        index_vec_input: np.ndarray,
        Sigma: np.ndarray,
        z: np.ndarray,
        tau: float,
        p_S: int,
    ) -> float:
        """Likehood of outlier model.

        Args:
            index_vec_input (np.ndarray): The index vector.
            Sigma (np.ndarray): The Sigma matrix.
            z (np.ndarray): The z vector.
            tau (float): The tau value.
            p_S (int): The number of SNPs.

        Returns:
            float: The likelihood of a model.

        Examples:
            >>> index_vec_input = np.array([1, 2, 3])
            >>> Sigma = np.array([[1, 0.5, 0.2], [0.5, 1, 0.3], [0.2, 0.3, 1]])
            >>> z = np.array([0.1, 0.2, 0.3])
            >>> tau = 1 / 0.05**2
            >>> p_S = 3
            >>> np.round(CARMA._outlier_ind_Normal_marginal_external(index_vec_input, Sigma, z, tau, p_S),decimals=5)
            -8.8497
        """
        index_vec = index_vec_input - 1

        Sigma_S = Sigma[np.ix_(index_vec, index_vec)]
        A = tau * np.eye(p_S)

        Sigma_S_I_inv = pinv(Sigma_S + A, rtol=0.00001)
        Sigma_S_inv = pinv(Sigma_S, rtol=0.00001)

        det_S = np.abs(det(Sigma_S_inv))
        det_I_S = np.abs(det(Sigma_S_I_inv))

        sub_z = z[index_vec]
        zSigmaz_S = np.dot(sub_z, np.dot(Sigma_S_inv, sub_z))
        zSigmaz_I_S = np.dot(sub_z, np.dot(Sigma_S_I_inv, sub_z))

        b = 0.5 * (np.log(det_S) + np.log(det_I_S)) - 0.5 * (zSigmaz_S - zSigmaz_I_S)
        results = b

        return results

    @staticmethod
    def _add_function(S_sub: np.ndarray, y: Any) -> np.ndarray:
        """Concatenate two arrays and sort the result.

        Args:
            S_sub (np.ndarray): The first array.
            y (Any): The second array.

        Returns:
            np.ndarray: The concatenated and sorted array.

        Examples:
            >>> S_sub = np.array([3, 4])
            >>> y = np.array([1, 2])
            >>> CARMA._add_function(S_sub, y)
            array([[1, 2, 3],
                   [1, 2, 4]])
        """
        return np.array([np.sort(np.concatenate(([x], y))) for x in S_sub])

    @staticmethod
    def _set_gamma_func_base(S: Any, p: int) -> dict[int, np.ndarray]:
        """Creates a dictionary of sets of configurations assuming no conditional set.

        Args:
            S (Any): The input set.
            p (int): The number of SNPs.

        Returns:
            dict[int, np.ndarray]: A dictionary of sets of configurations.

        Examples:
        >>> S = [0,1]
        >>> p = 4
        >>> CARMA._set_gamma_func_base(S, p)
        {0: array([[0],
               [1]]), 1: array([[0, 1, 2],
               [0, 1, 3]]), 2: array([[0, 2],
               [0, 3],
               [1, 2],
               [1, 3]])}

        >>> S = [0]
        >>> p = 2
        >>> CARMA._set_gamma_func_base(S, p)
        {0: None, 1: array([[0, 1]]), 2: array([[1]])}

        >>> S = []
        >>> p = 2
        >>> CARMA._set_gamma_func_base(S, p)
        {0: None, 1: array([[0],
               [1]]), 2: None}
        """
        set_gamma: dict[int, Any] = {}

        if len(S) == 0:
            set_gamma[0] = None
            set_gamma[1] = np.arange(0, p).reshape(-1, 1)
            set_gamma[2] = None

        if len(S) == 1:
            S_sub = np.setdiff1d(np.arange(0, p), S)
            set_gamma[0] = None
            set_gamma[1] = CARMA._add_function(S_sub, S)
            set_gamma[2] = S_sub.reshape(-1, 1)

        if len(S) > 1:
            S_sub = np.setdiff1d(np.arange(0, p), S)
            S = np.sort(S)
            set_gamma[0] = np.array(list(combinations(S, len(S) - 1)))
            set_gamma[1] = CARMA._add_function(S_sub, S)
            xs = np.vstack([CARMA._add_function(S_sub, row) for row in set_gamma[0]])
            set_gamma[2] = xs

        return set_gamma

    @staticmethod
    def _set_gamma_func_conditional(
        input_S: Any, condition_index: list[int], p: int
    ) -> dict[int, np.ndarray]:
        """Creates a dictionary of sets of configurations assuming conditional set.

        Args:
            input_S (Any): The input set.
            condition_index (list[int]): The conditional set.
            p (int): The number of SNPs.

        Returns:
            dict[int, np.ndarray]: A dictionary of sets of configurations.

        Examples:
        >>> input_S = [0,1,2]
        >>> condition_index = [2]
        >>> p = 4
        >>> CARMA._set_gamma_func_conditional(input_S, condition_index, p)
        {0: array([[0],
               [1]]), 1: array([[0, 1, 3]]), 2: array([[0, 3],
               [1, 3]])}
        """
        set_gamma: dict[int, Any] = {}
        S = np.setdiff1d(input_S, condition_index)

        # set of gamma-
        if len(S) == 0:
            S_sub = np.setdiff1d(np.arange(0, p), condition_index)
            set_gamma[0] = None
            set_gamma[1] = S_sub.reshape(-1, 1)
            set_gamma[2] = None

        if len(S) == 1:
            S_sub = np.setdiff1d(np.arange(0, p), input_S)
            set_gamma[0] = None
            set_gamma[1] = CARMA._add_function(S_sub, S)
            set_gamma[2] = S_sub.reshape(-1, 1)

        if len(S) > 1:
            S_sub = np.setdiff1d(np.arange(0, p), input_S)
            S = np.sort(S)
            set_gamma[0] = np.array(list(combinations(S, len(S) - 1)))
            set_gamma[1] = CARMA._add_function(S_sub, S)
            xs = np.vstack([CARMA._add_function(S_sub, row) for row in set_gamma[0]])
            set_gamma[2] = xs

        return set_gamma

    @staticmethod
    def _set_gamma_func(
        input_S: Any, p: int, condition_index: list[int] | None = None
    ) -> dict[int, np.ndarray]:
        """Creates a dictionary of sets of configurations.

        Args:
            input_S (Any): The input set.
            p (int): The number of SNPs.
            condition_index (list[int] | None): The conditional set. Defaults to None.

        Returns:
            dict[int, np.ndarray]: A dictionary of sets of configurations.

        Examples:
        >>> input_S = [0,1,2]
        >>> condition_index=[2]
        >>> p = 4
        >>> CARMA._set_gamma_func(input_S, p, condition_index)
        {0: array([[0],
               [1]]), 1: array([[0, 1, 3]]), 2: array([[0, 3],
               [1, 3]])}
        """
        if condition_index is None:
            results = CARMA._set_gamma_func_base(input_S, p)
        else:
            results = CARMA._set_gamma_func_conditional(input_S, condition_index, p)
        return results

    @staticmethod
    def _index_fun_internal(x: np.ndarray) -> str:
        """Convert an array of causal SNP indexes to comma-separated string.

        Args:
            x (np.ndarray): The input array.

        Returns:
            str: The comma-separated string.

        Examples:
        >>> x = np.array([1,2,3])
        >>> CARMA._index_fun_internal(x)
        '1,2,3'
        """
        y = np.sort(x)
        y = y.astype(str)
        return ",".join(y)

    @staticmethod
    def _index_fun(y: np.ndarray) -> np.ndarray:
        """Convert an array of causal SNP indexes to comma-separated string.

        Args:
            y (np.ndarray): The input array.

        Returns:
            np.ndarray: The comma-separated string.

        Examples:
        >>> y = np.array([[1,2,3],[4,5,6]])
        >>> CARMA._index_fun(y)
        array(['1,2,3', '4,5,6'], dtype='<U5')
        """
        return np.array([CARMA._index_fun_internal(x) for x in y])

    @staticmethod
    def _ridge_fun(
        x: float,
        Sigma: np.ndarray,
        modi_ld_S: np.ndarray,
        test_S: np.ndarray,
        z: np.ndarray,
        outlier_tau: float,
        outlier_likelihood: Any,
    ) -> float:
        """Estimate the matrix shrinkage parameter for outlier detection.

        Args:
            x (float): The input parameter.
            Sigma (np.ndarray): The Sigma matrix.
            modi_ld_S (np.ndarray): The modi_ld_S matrix.
            test_S (np.ndarray): The test_S matrix.
            z (np.ndarray): The z vector.
            outlier_tau (float): The outlier_tau value.
            outlier_likelihood (Any): The outlier_likelihood function.

        Returns:
            float: The estimated matrix shrinkage parameter.

        Examples:
        >>> x = 0.5
        >>> Sigma = np.array([[1, 0.5, 0.2], [0.5, 1, 0.3], [0.2, 0.3, 1]])
        >>> modi_ld_S = np.array([[1, 0.5], [0.5, 1]])
        >>> test_S = np.array([1, 2])
        >>> z = np.array([0.1, 0.2, 0.3])
        >>> outlier_tau = 1 / 0.05**2
        >>> outlier_likelihood = CARMA._outlier_ind_Normal_marginal_external
        >>> np.round(CARMA._ridge_fun(x, Sigma, modi_ld_S, test_S, z, outlier_tau, outlier_likelihood),decimals=5)
        6.01486
        """
        temp_Sigma = Sigma.copy()
        temp_ld_S = x * modi_ld_S + (1 - x) * np.eye(len(modi_ld_S))
        temp_Sigma[np.ix_(test_S, test_S)] = temp_ld_S
        return -outlier_likelihood(
            index_vec_input=test_S + 1,
            Sigma=temp_Sigma,
            z=z,
            tau=outlier_tau,
            p_S=len(test_S),
        )

    @staticmethod
    def _prior_dist(t: str, lambda_val: float, p: int) -> float:
        """Estimate the priors for the given configurations.

        Args:
            t (str): The input string for the given configuration.
            lambda_val (float): The lambda value.
            p (int): The number of SNPs.

        Returns:
            float: The estimated prior.

        Examples:
        >>> t = "1,2,3"
        >>> lambda_val = 1
        >>> p = 4
        >>> np.round(CARMA._prior_dist(t, lambda_val, p),decimals=5)
        -3.17805
        """
        index_array = t.split(",")
        dim_model = len(index_array)
        if t == "":
            dim_model = 0
        return (
            dim_model * np.log(lambda_val) + lgamma(p - dim_model + 1) - lgamma(p + 1)
        )

    @staticmethod
    def _PIP_func(
        likeli: pd.DataFrame, model_space: pd.DataFrame, p: int, num_causal: int
    ) -> np.ndarray:
        """Estimates the posterior inclusion probabilities (PIPs) for all SNPs.

        Args:
            likeli (pd.DataFrame): The marginal likelihoods.
            model_space (pd.DataFrame): The corresponding model space.
            p (int): The number of SNPs.
            num_causal (int): The maximal number of causal SNPs.

        Returns:
            np.ndarray: The posterior inclusion probabilities (PIPs) for all SNPs.

        Examples:
        >>> likeli = pd.DataFrame([10, 10, 5,11,0], columns=['likeli']).squeeze()
        >>> model_space = pd.DataFrame(['0', '1', '2','0,1',''], columns=['config']).squeeze()
        >>> p = 3
        >>> num_causal = 2
        >>> CARMA._PIP_func(likeli, model_space, p, num_causal)
        array([0.7869271, 0.7869271, 0.001426 ])
        """
        likeli = likeli.reset_index(drop=True)
        model_space = model_space.reset_index(drop=True)

        model_space_matrix = np.zeros((len(model_space), p), dtype=int)

        for i in range(len(model_space)):
            if model_space.iloc[i] != "":
                ind = list(map(int, model_space.iloc[i].split(",")))
                if len(ind) > 0:
                    model_space_matrix[i, ind] = 1

        infi_index = np.where(np.isinf(likeli))[0]
        if len(infi_index) != 0:
            likeli = likeli.drop(infi_index).reset_index(drop=True)
            model_space_matrix = np.delete(model_space_matrix, infi_index, axis=0)

        na_index = np.where(np.isnan(likeli))[0]
        if len(na_index) != 0:
            likeli = likeli.drop(na_index).reset_index(drop=True)
            model_space_matrix = np.delete(model_space_matrix, na_index, axis=0)

        row_sums = np.sum(model_space_matrix, axis=1)
        model_space_matrix = model_space_matrix[row_sums <= num_causal]
        likeli = likeli[row_sums <= num_causal]

        aa = likeli - max(likeli)
        prob_sum = np.sum(np.exp(aa))

        result_prob = np.zeros(p)
        for i in range(p):
            result_prob[i] = (
                np.sum(np.exp(aa[model_space_matrix[:, i] == 1])) / prob_sum
            )

        return result_prob

    @staticmethod
    def _MCS_modified(  # noqa: C901
        z: np.ndarray,
        ld_matrix: np.ndarray,
        Max_Model_Dim: int = 10_000,
        lambda_val: float = 1,
        num_causal: int = 10,
        outlier_switch: bool = True,
        input_conditional_S_list: list[int] | None = None,
        tau: float = 1 / 0.05**2,
        epsilon: float = 1e-3,
        inner_all_iter: int = 10,
        outlier_BF_index: float | None = None,
    ) -> dict[str, Any]:
        """Modified Monte Carlo shotgun sampling (MCS) algorithm.

        Args:
            z (np.ndarray): Numeric vector representing z-scores.
            ld_matrix (np.ndarray): Numeric matrix representing the linkage disequilibrium (LD) matrix.
            Max_Model_Dim (int): Maximum allowed dimension for the causal models.
            lambda_val (float): Regularization parameter controlling the strength of the L1 penalty.
            num_causal (int): Maximal number of causal variants to be selected in the final model.
            outlier_switch (bool): Whether to consider outlier detection in the analysis.
            input_conditional_S_list (list[int] | None): The conditional set. Defaults to None.
            tau (float): Tuning parameter controlling the level of shrinkage of the LD matrix.
            epsilon (float): Threshold for convergence in CARMA iterations.
            inner_all_iter (int): The number of inner iterations in each CARMA iteration.
            outlier_BF_index (float | None): Bayes Factor threshold for identifying outliers. Defaults to None.

        Returns:
            dict[str, Any]: A dictionary containing the following results:
                - B_list: A dataframe containing the marginal likelihoods and the corresponding model space.
                - conditional_S_list: A list of outliers.

        Examples:
        >>> z = np.array([0.1, 0.2, 0.3])
        >>> ld_matrix = np.array([[1, 0.5, 0.2], [0.5, 1, 0.3], [0.2, 0.3, 1]])
        >>> Max_Model_Dim = 10_000
        >>> lambda_val = 1

        >>> num_causal = 10
        >>> outlier_switch = True
        """
        p = len(z)
        marginal_likelihood = CARMA._ind_Normal_fixed_sigma_marginal_external
        tau_sample = tau
        if outlier_switch:
            outlier_likelihood = CARMA._outlier_ind_Normal_marginal_external
            outlier_tau = tau

        B = Max_Model_Dim
        stored_bf = 0
        Sigma = ld_matrix

        S = []

        null_model = ""
        null_margin = CARMA._prior_dist(null_model, lambda_val=lambda_val, p=p)

        B_list = pd.DataFrame({"set_gamma_margin": [null_margin], "matrix_gamma": [""]})

        if input_conditional_S_list is None:
            conditional_S = []
        else:
            conditional_S = input_conditional_S_list
            S = conditional_S

        for _i in range(0, inner_all_iter):
            for _j in range(0, 10):
                set_gamma = CARMA._set_gamma_func(
                    input_S=S, p=p, condition_index=conditional_S
                )

                if conditional_S is None:
                    working_S = S
                else:
                    working_S = np.sort(np.setdiff1d(S, conditional_S)).astype(int)

                set_gamma_margin: list[Any] = [None, None, None]
                set_gamma_prior: list[Any] = [None, None, None]
                matrix_gamma: list[Any] = [None, None, None]

                for i in range(0, len(set_gamma)):
                    if set_gamma[i] is not None:
                        matrix_gamma[i] = CARMA._index_fun(set_gamma[i])
                        p_S = set_gamma[i].shape[1]
                        set_gamma_margin[i] = np.apply_along_axis(
                            marginal_likelihood,
                            1,
                            set_gamma[i] + 1,
                            Sigma=Sigma,
                            z=z,
                            tau=tau_sample,
                            p_S=p_S,
                        )
                        set_gamma_prior[i] = np.array(
                            [
                                CARMA._prior_dist(model, lambda_val=lambda_val, p=p)
                                for model in matrix_gamma[i]
                            ]
                        )
                        set_gamma_margin[i] = set_gamma_prior[i] + set_gamma_margin[i]
                    else:
                        set_gamma_margin[i] = np.array(null_margin)
                        set_gamma_prior[i] = 0
                        matrix_gamma[i] = np.array(null_model)

                columns = ["set_gamma_margin", "matrix_gamma"]
                add_B = pd.DataFrame(columns=columns)

                for i in range(len(set_gamma)):
                    if isinstance(set_gamma_margin[i].tolist(), list):
                        new_row = pd.DataFrame(
                            {
                                "set_gamma_margin": set_gamma_margin[i].tolist(),
                                "matrix_gamma": matrix_gamma[i].tolist(),
                            }
                        )
                        add_B = pd.concat([add_B, new_row], ignore_index=True)
                    else:
                        new_row = pd.DataFrame(
                            {
                                "set_gamma_margin": [set_gamma_margin[i].tolist()],
                                "matrix_gamma": [matrix_gamma[i].tolist()],
                            }
                        )
                        add_B = pd.concat([add_B, new_row], ignore_index=True)

                # Add visited models into the storage space of models
                B_list = pd.concat([B_list, add_B], ignore_index=True)
                B_list = B_list.drop_duplicates(
                    subset="matrix_gamma", ignore_index=True
                )
                B_list = B_list.sort_values(
                    by="set_gamma_margin", ignore_index=True, ascending=False
                )

                if len(working_S) == 0:
                    # Create a DataFrame set.star
                    set_star = pd.DataFrame(
                        {
                            "set_index": [0, 1, 2],
                            "gamma_set_index": [np.nan, np.nan, np.nan],
                            "margin": [np.nan, np.nan, np.nan],
                        }
                    )

                    # Assuming set.gamma.margin and current.log.margin are defined
                    aa = set_gamma_margin[1]
                    aa = aa - aa[np.argmax(aa)]

                    min_half_len = min(len(aa), floor(p / 2))
                    decr_ind = np.argsort(np.exp(aa))[::-1]
                    decr_half_ind = decr_ind[:min_half_len]

                    probs = np.exp(aa)[decr_half_ind]

                    chosen_index = np.random.choice(
                        decr_half_ind, 1, p=probs / np.sum(probs)
                    )
                    set_star.at[1, "gamma_set_index"] = chosen_index[0]
                    set_star.at[1, "margin"] = set_gamma_margin[1][chosen_index[0]]

                    S = set_gamma[1][chosen_index[0]].tolist()

                else:
                    set_star = pd.DataFrame(
                        {
                            "set_index": [0, 1, 2],
                            "gamma_set_index": [np.nan, np.nan, np.nan],
                            "margin": [np.nan, np.nan, np.nan],
                        }
                    )
                    for i in range(0, 3):
                        aa = set_gamma_margin[i]
                        if np.size(aa) > 1:
                            aa = aa - aa[np.argmax(aa)]
                            chosen_index = np.random.choice(
                                range(0, np.size(set_gamma_margin[i])),
                                1,
                                p=np.exp(aa) / np.sum(np.exp(aa)),
                            )
                            set_star.at[i, "gamma_set_index"] = chosen_index
                            set_star.at[i, "margin"] = set_gamma_margin[i][chosen_index]
                        else:
                            set_star.at[i, "gamma_set_index"] = 0
                            set_star.at[i, "margin"] = set_gamma_margin[i]

                    if outlier_switch:
                        for i in range(1, len(set_gamma)):
                            test_log_BF: float = 100
                            while True:
                                aa = set_gamma_margin[i]
                                aa = aa - aa[np.argmax(aa)]
                                chosen_index = np.random.choice(
                                    range(0, np.size(set_gamma_margin[i])),
                                    1,
                                    p=np.exp(aa) / np.sum(np.exp(aa)),
                                )
                                set_star.at[i, "gamma_set_index"] = chosen_index
                                set_star.at[i, "margin"] = set_gamma_margin[i][
                                    chosen_index
                                ]

                                test_S = set_gamma[i][int(chosen_index), :]

                                modi_Sigma = Sigma.copy()
                                if np.size(test_S) > 1:
                                    modi_ld_S = modi_Sigma[test_S][:, test_S]

                                    result = minimize_scalar(
                                        CARMA._ridge_fun,
                                        bounds=(0, 1),
                                        args=(
                                            Sigma,
                                            modi_ld_S,
                                            test_S,
                                            z,
                                            outlier_tau,
                                            outlier_likelihood,
                                        ),
                                        method="bounded",
                                    )
                                    modi_ld_S = result.x * modi_ld_S + (
                                        1 - result.x
                                    ) * np.eye(len(modi_ld_S))

                                    modi_Sigma[np.ix_(test_S, test_S)] = modi_ld_S

                                    test_log_BF = outlier_likelihood(
                                        test_S + 1, Sigma, z, outlier_tau, len(test_S)
                                    ) - outlier_likelihood(
                                        test_S + 1,
                                        modi_Sigma,
                                        z,
                                        outlier_tau,
                                        len(test_S),
                                    )
                                    test_log_BF = -np.abs(test_log_BF)

                                if np.exp(test_log_BF) < outlier_BF_index:
                                    set_gamma[i] = np.delete(
                                        set_gamma[i],
                                        int(set_star["gamma_set_index"][i]),
                                        axis=0,
                                    )
                                    set_gamma_margin[i] = np.delete(
                                        set_gamma_margin[i],
                                        int(set_star["gamma_set_index"][i]),
                                        axis=0,
                                    )
                                    conditional_S = np.concatenate(
                                        [conditional_S, np.setdiff1d(test_S, working_S)]
                                    )
                                    conditional_S = (
                                        np.unique(conditional_S).astype(int).tolist()
                                    )
                                else:
                                    break

                    if len(working_S) == num_causal:
                        set_star = set_star.drop(1)
                        aa = set_star["margin"] - max(set_star["margin"])
                        sec_sample = np.random.choice(
                            [0, 2], 1, p=np.exp(aa) / np.sum(np.exp(aa))
                        )
                        ind_sec = int(
                            set_star["gamma_set_index"][
                                set_star["set_index"] == int(sec_sample)
                            ]
                        )
                        S = set_gamma[sec_sample[0]][ind_sec].tolist()
                    else:
                        aa = set_star["margin"] - max(set_star["margin"])
                        sec_sample = np.random.choice(
                            range(0, 3), 1, p=np.exp(aa) / np.sum(np.exp(aa))
                        )
                        if set_gamma[sec_sample[0]] is not None:
                            S = set_gamma[sec_sample[0]][
                                int(set_star["gamma_set_index"][sec_sample[0]])
                            ].tolist()
                        else:
                            sec_sample = np.random.choice(
                                range(1, 3),
                                1,
                                p=np.exp(aa)[[1, 2]] / np.sum(np.exp(aa)[[1, 2]]),
                            )
                            S = set_gamma[sec_sample[0]][
                                int(set_star["gamma_set_index"][sec_sample[0]])
                            ].tolist()

                for item in conditional_S:
                    if item not in S:
                        S.append(item)
            # END h_ind loop
            #
            if conditional_S is not None:
                all_c_index = []
                index_array = [s.split(",") for s in B_list["matrix_gamma"]]
                for tt in conditional_S:
                    tt_str = str(tt)
                    ind = [
                        i for i, sublist in enumerate(index_array) if tt_str in sublist
                    ]
                    all_c_index.extend(ind)

                all_c_index = list(set(all_c_index))

                if len(all_c_index) > 0:
                    temp_B_list = B_list.copy()
                    temp_B_list = B_list.drop(all_c_index)
                else:
                    temp_B_list = B_list.copy()
            else:
                temp_B_list = B_list.copy()

            result_B_list = temp_B_list[: min(int(B), len(temp_B_list))]

            rb1 = result_B_list["set_gamma_margin"]

            difference = abs(rb1[: (len(rb1) // 4)].mean() - stored_bf)

            if difference < epsilon:
                break
            else:
                stored_bf = rb1[: (len(rb1) // 4)].mean()

        out = {"B_list": result_B_list, "conditional_S_list": conditional_S}

        return out
