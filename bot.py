import pandas as pd
import requests
import json
import pymsteams
from datetime import datetime
import pytz
import sqlalchemy
import sqlite3
from datetime import datetime
from plyer import notification
import math


def runBots():
    ticket_url = 'https://aais.zendesk.com/api/v2/search.json?query=type:ticket group_id:360008987952'
    DATA_LOCATION = "sqlite:///escalations.sqlite"
    engine = sqlalchemy.create_engine(DATA_LOCATION)
    connection = sqlite3.connect('escalations.sqlite')
    cursor = connection.cursor()
    tableName = "escalations_tickets"
    myTeamsMessage = pymsteams.connectorcard('https://adastra1.webhook.office.com/webhookb2/b94a3828-160c-4911-b427-ec738137737a@735aebe8-b1ad-4100-8c92-6b3384929c66/IncomingWebhook/6334b3e47aa348409daffb5c4a7f3247/f1bea454-709e-4878-8767-1a80d700c4a9')
    myTeamsMessageAddition = pymsteams.connectorcard('https://adastra1.webhook.office.com/webhookb2/b94a3828-160c-4911-b427-ec738137737a@735aebe8-b1ad-4100-8c92-6b3384929c66/IncomingWebhook/6334b3e47aa348409daffb5c4a7f3247/f1bea454-709e-4878-8767-1a80d700c4a9')
    #myTeamsMessage = pymsteams.connectorcard('https://adastra1.webhook.office.com/webhookb2/b94a3828-160c-4911-b427-ec738137737a@735aebe8-b1ad-4100-8c92-6b3384929c66/IncomingWebhook/defe5f26d74f4a5fb2f99e310aae7aa5/f1bea454-709e-4878-8767-1a80d700c4a9')
    #myTeamsMessageAddition = pymsteams.connectorcard('https://adastra1.webhook.office.com/webhookb2/b94a3828-160c-4911-b427-ec738137737a@735aebe8-b1ad-4100-8c92-6b3384929c66/IncomingWebhook/defe5f26d74f4a5fb2f99e310aae7aa5/f1bea454-709e-4878-8767-1a80d700c4a9')

    myTeamsMessage.title("Ticket Updates")
    myTeamsMessageAddition.title("Additional Updates!!")
    myMessageSection = pymsteams.cardsection()
    user = "kagata@aais.com"
    pwd = "Bchan4lyf!"
    response = requests.get(ticket_url, auth=(user, pwd))
    if response.status_code != 200:
        print('Not connecting to Zendesk')
    def getTenantNames(*orgIds):
        org_frame = []
        refine_org_frame = []
        for orgId in orgIds:
            organization_url = 'https://aais.zendesk.com/api/v2/organizations/{orgId}'.format(orgId = orgId)
            response = requests.get(organization_url, auth=(user, pwd))
            if response.status_code != 200:
                print("no org id")
                org_frame.append({"organization":{"id":0, "name":"no tenant assigned"}})
            else:
                organizationList = response.json()
                org_frame.append(organizationList)
        for item in org_frame:
            refine_org_frame.append(item['organization'])
        #org_frame = pd.json_normalize(org_frame)[['id','name']]
        org_dataframe = pd.json_normalize(refine_org_frame)[['id','name']]
        return org_dataframe
    def listTenantNames(tenantIds):
        tenantNameMapping = []
        for tenant in tenantIds:
            tenantNameMapping.append(tenant)
        tenantId = getTenantNames(*tenantNameMapping)['id']
        tenantName = getTenantNames(*tenantNameMapping)['name']
        tenantNameMapping = dict(zip(tenantId, tenantName))
        print(tenantNameMapping)
        return tenantNameMapping
    def idToNames(ids):
        return ids.map(listTenantNames(ids))
    def getUnixTimes(times):
        unixList = []
        for time in times:
            unixList.append(datetime.timestamp(pd.to_datetime(time)))
        return unixList
    def getUnixMapping(a):
        timeList = sorted(a)
        unixes = getUnixTimes(a)
        unix = sorted(unixes)
        return dict(zip(timeList, unix))
    def convertTimeZones(timezones):
        originalTimeZones = []
        newTimeZones = []
        for timezone in timezones:
            originalTimeZones.append(timezone)
        for timezone in originalTimeZones:
            newTimeZones.append(str(pd.to_datetime(timezone).astimezone('US/Central')))
        originalTimeZones = sorted(originalTimeZones)
        newTimeZones = sorted(newTimeZones)
        return dict(zip(originalTimeZones, newTimeZones))
    def mapTimeZones(timezones):
        useThisTimezones = timezones.map(convertTimeZones(timezones))
        return useThisTimezones
    tickets = response.json()
    dataframe = []
    for ticket in tickets['results']:
        dataframe.append(ticket)
    ticket_frame = pd.json_normalize(dataframe)[['id','group_id','status','created_at','updated_at','subject','description','priority','organization_id','url']]
    esc_ticket = ticket_frame.query('group_id==360008987952 and (status=="open" or status=="pending" or status=="hold" or status=="new")')
    if len(esc_ticket) == 0:

        queryUpdatedDate = '''
        SELECT id, updated_at FROM escalations_tickets
        '''

        cursor.execute(queryUpdatedDate)
        dbeaverObject = dict(list(cursor.fetchall()))
        dbeaverLen = len(dbeaverObject)
        if dbeaverLen > 0:

            queryToDelete = """
            Delete from escalations_tickets
            """
            esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
            cursor.execute(queryToDelete)
            print("There is no active ticket in the queue. Deleting the ticket in dbeaver")
            myTeamsMessage.text("Awesomesauce! 0 ticket in the queue!")
            myTeamsMessage.send()
            #notification.notify(
            #        title = 'Escalation-bot',
            #        message = 'No tickets in the queue',
            #        app_icon = None,
            #        timeout = 10
            #    )
            connection.close()

        else: 
            print("There is no active ticket in the queue.")
            connection.close()

    else:
        esc_ticket['organization_id'] = esc_ticket['organization_id'].fillna(0)
        esc_ticket['organization_id'] = idToNames(esc_ticket['organization_id'])
        esc_ticket = esc_ticket.rename(columns={"organization_id":"tenant_name"})
        timeMapping = getUnixMapping(esc_ticket['updated_at'])
        esc_ticket['timestamp'] = esc_ticket['updated_at'].map(timeMapping)
        #esc_ticket['updated_at'] = tm.mapTimeZones(esc_ticket['updated_at'])
        esc_ticket['updated_at'] = mapTimeZones(esc_ticket['updated_at'])
        print(esc_ticket['updated_at'])
        queryToStore = """
        CREATE TABLE IF NOT EXISTS escalations_ticekts (
            id INT(200),
            group_id BIGINT(200),
            status VARCHAR(200),
            created_at VARCHAR(200),
            updated_at VARCHAR(200),
            subject VARCHAR(200),
            description VARCHAR(200),
            priority VARCHAR(200),
            tenant_name VARCHAR(200),
            timestamp TIMESTAMP,
            url VARCHAR(200)
            )
            """
        queryUpdatedDate = '''
        SELECT id, updated_at FROM escalations_tickets
        '''

        queryToBeDeletedInDbeaver = '''
        SELECT id, tenant_name from escalations_tickets
        '''

        ## To initially set up the database
        #esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
        #cursor.execute(queryToStore)
        
        cursor.execute(queryUpdatedDate)
        dbeaverObject = dict(list(cursor.fetchall()))
        print(dbeaverObject)
        dbeaverLen = len(dbeaverObject)
        print("this is  the length of the dbeaverObject", dbeaverLen)
        ##If dbeaver records were deleted due to no tickets, and new tickets came in, goes through this to recreate the table and update
        if dbeaverLen == 0:
            print("re-creating dbeaver table")
            esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
            cursor.execute(queryToStore)

            myTeamsMessage.text("New ticket(s)!")
            myMessageSection.addFact("Difference: ", len(esc_ticket))
            for ticket in esc_ticket['id']:
                url = esc_ticket.query("id == @ticket")['url'].to_string(index=False).replace("/api/v2","").replace(".json","")
                tenantName = esc_ticket.query("id == @ticket")['tenant_name'].to_string(index=False)
                myMessageSection.addFact("Links: ", url)
                myMessageSection.addFact("Tenant Name: ", tenantName)
            myTeamsMessage.addSection(myMessageSection)
            myTeamsMessage.send()

        ##If above does not apply, run as usual
        else:
            cursor.execute(queryToBeDeletedInDbeaver)
            dbeaverGoneObj = dict(list(cursor.fetchall()))
            print(dbeaverGoneObj)
            ##If there are same number of tickets in zendesk and dbeaver, then we check for updated_at values for any updates
            def checkUpdatedDates(stored):
                listOfTicketId = []
                for key in stored:
                    compareTickets = esc_ticket.query('id == @key')
                    listOfTicketId.append(compareTickets['id'].to_string(index=False))
            ##If there are same number of tickets at this point, it means no tickets got transferred or closed then got new tickets
                if len(listOfTicketId) == dbeaverLen:
                    print("No different tickets")
                    esc_ticket['previous_update'] = esc_ticket['id'].map(stored)
            ##If updated_at values are the same, no update
                    if esc_ticket['updated_at'].equals(esc_ticket['previous_update']):
                        print("No updates")

                        #myTeamsMessage.text("testing")
                        #myTeamsMessage.send()
                        connection.close()
                    #    notification.notify(
                    #    title = 'Escalation-bot',
                    #    message = 'No updates detected',
                    #    app_icon = None,
                    #    timeout = 10
                    #)
            ##Otherwise, there is some update
                    else:
                        print("There's some update")
                        ticketsToCheck = esc_ticket.query("updated_at != previous_update")
                        for ticketToCheck in ticketsToCheck['id']:
                            eachTicket = ticketsToCheck.query("id == @ticketToCheck")
                            print(eachTicket)
                            updatedTicketLink = eachTicket['url'].to_string(index=False)
                            refineUpdatedTicketLink = updatedTicketLink.replace("/api/v2","").replace(".json","")
                            print(refineUpdatedTicketLink)
                            updatedTicketId = eachTicket['id'].to_string(index=False)
                            print(updatedTicketId)
                            tenantName = eachTicket['tenant_name'].to_string(index=False)
                            print(tenantName)
                            newUpdate = eachTicket['updated_at'].to_string(index=False)
                            myMessageSection.addFact("Ticket ID: ", updatedTicketId)
                            myMessageSection.addFact("Link: ", refineUpdatedTicketLink)
                            myMessageSection.addFact("Updated at: ", newUpdate)
                            myMessageSection.addFact("Tenant Name: ", tenantName)
                        myTeamsMessage.addSection(myMessageSection)
                        myTeamsMessage.text("Updates were detected")
                        myTeamsMessage.send()
                        esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
                        cursor.execute(queryToStore)
                        connection.close()
                    #    notification.notify(
                    #    title = 'Escalation-bot',
                    #    message = 'There are some updates!',
                    #    app_icon = None,
                    #    timeout = 10
                    #)
            ##This is when there are same number of tickets, but ids are different
                else:
                    forgottenTicket = esc_ticket.query("id not in @listOfTicketId")
                    forgottenTicketId = forgottenTicket['id'].to_string(index=False)
                    forgottenTicketLink = forgottenTicket['url'].to_string(index=False).replace("/api/v2","").replace(".json","")
                    myMessageSection.addFact("Ticket id: ", forgottenTicketId)
                    myMessageSection.addFact("Link: ", forgottenTicketLink)
                    myTeamsMessage.text("There are same number of tickets, but one or more tickets in the queue may be different from previous check")
                    myTeamsMessage.addSection(myMessageSection)
                    myTeamsMessage.send()
                    esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
                    cursor.execute(queryToStore)
                    connection.close()
                    #notification.notify(
                    #    title = 'Escalation-bot',
                    #    message = 'There are some updates!',
                    #    app_icon = None,
                    #    timeout = 10
                    #)
            ##Run this to check for ticket updates even when there are different number of tickets in the queue
            def checkPureUpdates(stored):
                listOfTicketId = []
                for key in stored:
                    compareTickets = esc_ticket.query("id == @key")
                    listOfTicketId.append(compareTickets['id'].to_string(index=False))
                sameTickets = esc_ticket.query("id in @listOfTicketId")
                sameTickets['previous_update'] = sameTickets['id'].map(stored)
                if sameTickets['previous_update'].equals(sameTickets['updated_at']):
                    print("No updates")
                    esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
                    cursor.execute(queryToStore)
                    connection.close()
                    #notification.notify(
                    #    title = 'Escalation-bot',
                    #    message = 'Different number of tickets with no updates',
                    #    app_icon = None,
                    #    timeout = 10
                    #)
                else:
                    print("There's some update")
                    ticketsToCheck = sameTickets.query("updated_at != previous_update")
                    for ticketToCheck in ticketsToCheck['id']:
                        eachTicket = ticketsToCheck.query("id == @ticketToCheck")
                        updatedTicketLink = eachTicket['url'].to_string(index=False)
                        refineUpdatedTicketLink = updatedTicketLink.replace("/api/v2","").replace(".json","")
                        updatedTicketId = eachTicket['id'].to_string(index=False)
                        newUpdate = eachTicket['updated_at'].to_string(index=False)
                        tenantName = eachTicket['tenant_name'].to_string(index=False)
                        myMessageSection.addFact("Ticket ID: ", updatedTicketId)
                        myMessageSection.addFact("Link: ", refineUpdatedTicketLink)
                        myMessageSection.addFact("Updated at: ", newUpdate)
                        myMessageSection.addFact("Tenant Name: ", tenantName)
                    myTeamsMessageAddition.addSection(myMessageSection)
                    myTeamsMessageAddition.text("Additionally, some updates were detected on some tickets")
                    myTeamsMessageAddition.send()
                    esc_ticket.to_sql(tableName, engine, index=False, if_exists='replace')
                    cursor.execute(queryToStore)
                    connection.close()
                    #notification.notify(
                    #    title = 'Escalation-bot',
                    #    message = 'Different number of tickets AND updates',
                    #    app_icon = None,
                    #    timeout = 10
                    #)
            if len(esc_ticket) == dbeaverLen:
                print("Same number of tickets")
                checkUpdatedDates(dbeaverObject)
            ##If there are different number of tickets, then update
            else:
                print("Different number of tickets")
                if len(esc_ticket) > dbeaverLen:
                    diffLen = len(esc_ticket) - dbeaverLen
                    listOfIds = []
                    for ticketId in dbeaverObject:
                        listOfIds.append(ticketId)
                    missingTicket = esc_ticket.query("id not in @listOfIds")
                    myTeamsMessage.text("There are more tickets in the queue than last we checked!")
                    myMessageSection.addFact("Difference: ", diffLen)
                    for ticket in missingTicket['id']:
                        url = missingTicket.query("id == @ticket")['url'].to_string(index=False).replace("/api/v2","").replace(".json","")
                        tenantName = missingTicket.query("id == @ticket")['tenant_name'].to_string(index=False)
                        myMessageSection.addFact("Links: ", url)
                        myMessageSection.addFact("Tenant Name: ", tenantName)
                    myTeamsMessage.addSection(myMessageSection)
                    myTeamsMessage.send()
            ##check for ticket updates here
                    checkPureUpdates(dbeaverObject)
                    print("saved escalation tickets to dbeaver as ", tableName)
                if dbeaverLen > len(esc_ticket):
                    diffLen = dbeaverLen - len(esc_ticket)
                    listOfIdsInZd = []
                    listOfIdsInDb = []
                    for ticket in esc_ticket['id']:
                        listOfIdsInZd.append(ticket)
                    for id in dbeaverObject:
                        listOfIdsInDb.append(id)
                    goneId = list(set(listOfIdsInDb).difference(listOfIdsInZd))
                    print(goneId)
                    myMessageSection.addFact("Difference: ", diffLen)
                    for id in goneId:
                        ticketLink = 'https://aais.zendesk.com/api/v2/tickets/{ticket_id}'.format(ticket_id=id)
                        response = requests.get(ticketLink, auth=(user,pwd))
                        ticketUrl = response.json()['ticket']['url']
                        refineLink = ticketUrl.replace("/api/v2","").replace(".json","")
                        tenantName = dbeaverGoneObj[id]
                        myMessageSection.addFact("Links: ", refineLink)
                        myMessageSection.addFact("Tenant Name: ", tenantName)
                    myTeamsMessage.text("There are less tickets in the queue than last we checked!")
                    myTeamsMessage.addSection(myMessageSection)
                    myTeamsMessage.send()
            ##check for ticket updates here
                    checkPureUpdates(dbeaverObject)
                    print("saved escalation tickets to dbeaver as ", tableName)

runBots()
