/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.delete

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportDeleteRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<DeleteRollupRequest, DeleteResponse>(
    DeleteRollupAction.NAME, transportService, actionFilters, ::DeleteRollupRequest
) {

    override fun doExecute(task: Task, request: DeleteRollupRequest, actionListener: ActionListener<DeleteResponse>) {
        val deleteRequest = DeleteRequest(INDEX_MANAGEMENT_INDEX, request.id())
            .setRefreshPolicy(request.refreshPolicy)
        client.delete(deleteRequest, actionListener)
    }
}
